import time

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.regression import (
    DecisionTreeRegressor,
    GBTRegressor,
    LinearRegression,
    RandomForestRegressor,
)
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ml_features import build_business_features

SEED = 42
NUM_FOLDS = 3


def _feature_stages(numeric_cols: list, categorical_cols: list, scale: bool) -> list:
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
        # linear model is sensitive to feature scale, trees are not
        stages.append(StandardScaler(inputCol="raw_features", outputCol="features"))
    return stages


def _model_specs():
    lr = LinearRegression(solver="l-bfgs", maxIter=100)
    dt = DecisionTreeRegressor(seed=SEED)
    rf = RandomForestRegressor(seed=SEED)
    gbt = GBTRegressor(seed=SEED)

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
            .addGrid(rf.numTrees, [50])
            .addGrid(rf.maxDepth, [8, 12])
            .build()
        )),
        ("GBTRegressor", gbt, False, (
            ParamGridBuilder()
            .addGrid(gbt.maxIter, [50])
            .addGrid(gbt.maxDepth, [3, 5])
            .addGrid(gbt.stepSize, [0.1])
            .build()
        )),
    ]


def _feature_names(df, features_col: str = "features") -> list:
    attrs = df.schema[features_col].metadata["ml_attr"]["attrs"]
    flat = sorted((a for group in attrs.values() for a in group), key=lambda a: a["idx"])
    return [a["name"] for a in flat]


def _evaluate(predictions) -> dict:
    return {
        metric: RegressionEvaluator(metricName=metric).evaluate(predictions)
        for metric in ("rmse", "r2", "mae")
    }


def _format_params(param_map: dict) -> str:
    return ", ".join(f"{p.name}={v}" for p, v in sorted(param_map.items(), key=lambda kv: kv[0].name))


def _write_csv(spark: SparkSession, rows: list, columns: list, path: str):
    spark.createDataFrame(rows, columns).coalesce(1).write.mode("overwrite") \
        .option("header", True).csv(path)


def run_regression(spark: SparkSession, datasets: dict, output_dir: str):
    """Регресія business.stars: 4 алгоритми, крос-валідація, порівняння метрик."""
    print(f"\n{'=' * 60}")
    print(" REGRESSION — predicting business.stars")
    print(f"{'=' * 60}")

    data, numeric_cols, categorical_cols = build_business_features(datasets)
    train, test = data.randomSplit([0.8, 0.2], seed=SEED)
    train, test = train.cache(), test.cache()
    print(f" Train rows: {train.count():,} | Test rows: {test.count():,}")
    print(f" Features: {len(numeric_cols)} numeric + one-hot {categorical_cols}")
    train.select("label").summary("mean", "stddev", "min", "max").show()

    # baseline: always predict train mean — any useful model must beat it
    train_mean = train.agg(F.avg("label")).first()[0]
    baseline = _evaluate(test.withColumn("prediction", F.lit(train_mean)))
    metrics_rows = [(
        "Baseline (mean)", None, None, baseline["rmse"], baseline["r2"], baseline["mae"],
        0.0, f"prediction={train_mean:.4f}",
    )]
    cv_rows, importance_rows, curve_rows = [], [], []

    for name, estimator, scale, grid in _model_specs():
        print(f"\n--- {name}: {len(grid)} param sets x {NUM_FOLDS} folds ---")
        pipeline = Pipeline(stages=_feature_stages(numeric_cols, categorical_cols, scale) + [estimator])
        cv = CrossValidator(
            estimator=pipeline,
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

        best: PipelineModel = cv_model.bestModel
        best_params = _format_params(grid[cv_model.avgMetrics.index(min(cv_model.avgMetrics))])
        train_pred = best.transform(train)
        test_pred = best.transform(test)
        train_m, test_m = _evaluate(train_pred), _evaluate(test_pred)

        print(f"   best: [{best_params}]  fit time {train_time:.1f}s")
        print(f"   train rmse={train_m['rmse']:.4f} r2={train_m['r2']:.4f}")
        print(f"   test  rmse={test_m['rmse']:.4f} r2={test_m['r2']:.4f} mae={test_m['mae']:.4f}")

        metrics_rows.append((
            name, train_m["rmse"], train_m["r2"], test_m["rmse"], test_m["r2"], test_m["mae"],
            train_time, best_params,
        ))

        model = best.stages[-1]
        # StandardScaler drops attribute names, so read them from the assembler output
        names = _feature_names(test_pred, "raw_features" if scale else "features")

        if name == "LinearRegression":
            summary = model.summary
            print(f"   iterations: {summary.totalIterations}")
            for i, loss in enumerate(summary.objectiveHistory):
                curve_rows.append((name, i, "train_objective", float(loss)))
            weights = [float(w) for w in model.coefficients]
        else:
            weights = [float(w) for w in model.featureImportances.toArray()]

        top = sorted(zip(names, weights), key=lambda kv: abs(kv[1]), reverse=True)
        print("   top-10 features:", ", ".join(f"{n}={w:.3f}" for n, w in top[:10]))
        kind = "coefficient" if name == "LinearRegression" else "importance"
        importance_rows += [(name, n, kind, w) for n, w in top]

        if name == "GBTRegressor":
            # learning curve: loss after each boosting iteration on train vs test
            features_only = PipelineModel(best.stages[:-1])
            train_losses = model.evaluateEachIteration(features_only.transform(train), "squared")
            test_losses = model.evaluateEachIteration(features_only.transform(test), "squared")
            for i, (tr, te) in enumerate(zip(train_losses, test_losses), start=1):
                curve_rows.append((name, i, "train_squared_loss", float(tr)))
                curve_rows.append((name, i, "test_squared_loss", float(te)))

    metrics_cols = ["model", "train_rmse", "train_r2", "test_rmse", "test_r2", "test_mae",
                    "train_time_sec", "best_params"]
    metrics_df = spark.createDataFrame(metrics_rows, metrics_cols)

    print(f"\n{'=' * 60}")
    print(" REGRESSION — model comparison (test set)")
    print(f"{'=' * 60}")
    metrics_df.orderBy("test_rmse").select(
        "model",
        *[F.round(c, 4).alias(c) for c in metrics_cols[1:7]],
    ).show(truncate=False)

    metrics_df.coalesce(1).write.mode("overwrite").option("header", True) \
        .csv(f"{output_dir}/regression_metrics")
    _write_csv(spark, cv_rows, ["model", "params", "cv_avg_rmse"], f"{output_dir}/regression_cv_results")
    _write_csv(spark, importance_rows, ["model", "feature", "kind", "value"],
               f"{output_dir}/regression_feature_importance")
    _write_csv(spark, curve_rows, ["model", "iteration", "series", "value"],
               f"{output_dir}/regression_learning_curves")

    train.unpersist()
    test.unpersist()
    return metrics_df
