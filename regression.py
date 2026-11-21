import time

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.regression import (
    DecisionTreeRegressor,
    GBTRegressionModel,
    GBTRegressor,
    LinearRegression,
    LinearRegressionModel,
    RandomForestRegressor,
)
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ml_features import build_business_features

SEED = 42
NUM_FOLDS = 3
METRICS = ("rmse", "r2", "mae")


def feature_stages(numeric_cols: list, categorical_cols: list, scale: bool) -> list:
    """Index + one-hot encode categoricals and assemble everything into `features`."""
    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in categorical_cols
    ]
    encoder = OneHotEncoder(
        inputCols=[f"{c}_idx" for c in categorical_cols],
        outputCols=[f"{c}_ohe" for c in categorical_cols],
        handleInvalid="keep",
    )
    assembler = VectorAssembler(
        inputCols=numeric_cols + [f"{c}_ohe" for c in categorical_cols],
        outputCol="raw_features" if scale else "features",
    )
    stages = indexers + [encoder, assembler]
    if scale:
        # linear models are sensitive to feature scale, trees are not
        stages.append(StandardScaler(inputCol="raw_features", outputCol="features"))
    return stages


def _model_specs() -> list:
    """(name, estimator, needs_scaling, param_grid) for every compared algorithm."""
    lr = LinearRegression(solver="l-bfgs", maxIter=100)
    dt = DecisionTreeRegressor(seed=SEED)
    rf = RandomForestRegressor(seed=SEED, numTrees=50)
    gbt = GBTRegressor(seed=SEED, maxIter=50, stepSize=0.1)

    return [
        ("LinearRegression", lr, True, (
            ParamGridBuilder()
            .addGrid(lr.regParam, [0.0, 0.01, 0.1])
            .addGrid(lr.elasticNetParam, [0.0, 0.5])
            .build()
        )),
        ("DecisionTreeRegressor", dt, False, (
            ParamGridBuilder()
            .addGrid(dt.maxDepth, [5, 10])
            .addGrid(dt.minInstancesPerNode, [1, 20])
            .build()
        )),
        ("RandomForestRegressor", rf, False, (
            ParamGridBuilder()
            .addGrid(rf.maxDepth, [8, 12])
            .build()
        )),
        ("GBTRegressor", gbt, False, (
            ParamGridBuilder()
            .addGrid(gbt.maxDepth, [3, 5])
            .build()
        )),
    ]


def feature_names(df: DataFrame, features_col: str) -> list:
    """Read slot names of an assembled vector column from its ML metadata."""
    attrs = df.schema[features_col].metadata["ml_attr"]["attrs"]
    flat = sorted((a for group in attrs.values() for a in group), key=lambda a: a["idx"])
    return [a["name"] for a in flat]


def _evaluate(predictions: DataFrame) -> dict:
    return {
        metric: RegressionEvaluator(metricName=metric).evaluate(predictions)
        for metric in METRICS
    }


def _format_params(param_map: dict) -> str:
    items = sorted(param_map.items(), key=lambda kv: kv[0].name)
    return ", ".join(f"{p.name}={v}" for p, v in items)


def _write_csv(df: DataFrame, path: str):
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(path)


def _weights(model, names: list) -> list:
    """Return (kind, [(feature, weight), ...]) sorted by absolute weight."""
    if isinstance(model, LinearRegressionModel):
        kind, values = "coefficient", model.coefficients
    else:
        kind, values = "importance", model.featureImportances.toArray()
    pairs = sorted(zip(names, (float(v) for v in values)), key=lambda kv: abs(kv[1]), reverse=True)
    return kind, pairs


def _learning_curve(name: str, best: PipelineModel, train: DataFrame, test: DataFrame) -> list:
    """Loss per optimizer step (LR) or per boosting iteration on train and test (GBT)."""
    model = best.stages[-1]
    if isinstance(model, LinearRegressionModel):
        print(f"   iterations: {model.summary.totalIterations}")
        return [(name, i, "train_objective", float(loss))
                for i, loss in enumerate(model.summary.objectiveHistory)]

    if isinstance(model, GBTRegressionModel):
        features_only = PipelineModel(best.stages[:-1])
        train_losses = model.evaluateEachIteration(features_only.transform(train), "squared")
        test_losses = model.evaluateEachIteration(features_only.transform(test), "squared")
        rows = []
        for i, (tr, te) in enumerate(zip(train_losses, test_losses), start=1):
            rows.append((name, i, "train_squared_loss", float(tr)))
            rows.append((name, i, "test_squared_loss", float(te)))
        return rows

    return []


def run_regression(spark: SparkSession, datasets: dict, output_dir: str) -> DataFrame:
    """Predict business.stars with 4 algorithms, tune them with CV and compare on test."""
    print(f"\n{'=' * 60}")
    print(" REGRESSION — predicting business.stars")
    print(f"{'=' * 60}")

    data, numeric_cols, categorical_cols = build_business_features(datasets)
    train, test = data.randomSplit([0.8, 0.2], seed=SEED)
    train, test = train.cache(), test.cache()
    print(f" Train rows: {train.count():,} | Test rows: {test.count():,}")
    print(f" Features: {len(numeric_cols)} numeric + one-hot {categorical_cols}")
    train.select("label").summary("mean", "stddev", "min", "max").show()

    # baseline: always predict the train mean — any useful model must beat it
    train_mean = train.agg(F.avg("label")).first()[0]
    baseline = _evaluate(test.withColumn("prediction", F.lit(train_mean)))
    metrics_rows = [(
        "Baseline (mean)", None, None, baseline["rmse"], baseline["r2"], baseline["mae"],
        0.0, f"prediction={train_mean:.4f}",
    )]
    cv_rows, importance_rows, curve_rows = [], [], []

    for name, estimator, scale, grid in _model_specs():
        print(f"\n--- {name}: {len(grid)} param sets x {NUM_FOLDS} folds ---")
        cv = CrossValidator(
            estimator=Pipeline(stages=feature_stages(numeric_cols, categorical_cols, scale) + [estimator]),
            estimatorParamMaps=grid,
            evaluator=RegressionEvaluator(metricName="rmse"),
            numFolds=NUM_FOLDS,
            seed=SEED,
            parallelism=2,
        )

        start = time.time()
        cv_model = cv.fit(train)
        train_time = time.time() - start

        for params, avg_rmse in zip(grid, cv_model.avgMetrics):
            cv_rows.append((name, _format_params(params), float(avg_rmse)))
            print(f"   CV rmse={avg_rmse:.4f}  [{_format_params(params)}]")

        best = cv_model.bestModel
        best_params = _format_params(grid[cv_model.avgMetrics.index(min(cv_model.avgMetrics))])
        test_pred = best.transform(test)
        train_m, test_m = _evaluate(best.transform(train)), _evaluate(test_pred)

        print(f"   best: [{best_params}]  fit time {train_time:.1f}s")
        print(f"   train rmse={train_m['rmse']:.4f} r2={train_m['r2']:.4f}")
        print(f"   test  rmse={test_m['rmse']:.4f} r2={test_m['r2']:.4f} mae={test_m['mae']:.4f}")
        metrics_rows.append((
            name, train_m["rmse"], train_m["r2"], test_m["rmse"], test_m["r2"], test_m["mae"],
            train_time, best_params,
        ))

        # StandardScaler drops slot names, so read them from the assembler output
        names = feature_names(test_pred, "raw_features" if scale else "features")
        kind, weights = _weights(best.stages[-1], names)
        print("   top-10 features:", ", ".join(f"{n}={w:.3f}" for n, w in weights[:10]))
        importance_rows += [(name, n, kind, w) for n, w in weights]

        curve_rows += _learning_curve(name, best, train, test)

    metrics_cols = ["model", "train_rmse", "train_r2", "test_rmse", "test_r2", "test_mae",
                    "train_time_sec", "best_params"]
    metrics_df = spark.createDataFrame(metrics_rows, metrics_cols)

    print(f"\n{'=' * 60}")
    print(" REGRESSION — model comparison (test set)")
    print(f"{'=' * 60}")
    metrics_df.orderBy("test_rmse").select(
        "model", *[F.round(c, 4).alias(c) for c in metrics_cols[1:7]],
    ).show(truncate=False)

    _write_csv(metrics_df, f"{output_dir}/regression_metrics")
    _write_csv(spark.createDataFrame(cv_rows, ["model", "params", "cv_avg_rmse"]),
               f"{output_dir}/regression_cv_results")
    _write_csv(spark.createDataFrame(importance_rows, ["model", "feature", "kind", "value"]),
               f"{output_dir}/regression_feature_importance")
    _write_csv(spark.createDataFrame(curve_rows, ["model", "iteration", "series", "value"]),
               f"{output_dir}/regression_learning_curves")

    train.unpersist()
    test.unpersist()
    return metrics_df
