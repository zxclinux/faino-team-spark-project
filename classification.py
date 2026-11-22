import time

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import (
    DecisionTreeClassifier,
    GBTClassificationModel,
    GBTClassifier,
    LogisticRegression,
    LogisticRegressionModel,
    RandomForestClassifier,
)
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ml_features import build_business_features
from regression import _format_params, _write_csv, feature_names, feature_stages

SEED = 42
NUM_FOLDS = 3
CLOSED = 0.0  # minority class we care about most

# overall metrics (weighted by class support)
OVERALL_METRICS = ("accuracy", "weightedPrecision", "weightedRecall", "f1")
# per-class metrics for the "closed" class
CLOSED_METRICS = {
    "precision_closed": "precisionByLabel",
    "recall_closed": "recallByLabel",
    "f1_closed": "fMeasureByLabel",
}
METRIC_KEYS = list(OVERALL_METRICS) + list(CLOSED_METRICS) + ["auc"]


def build_classification_data(datasets: dict):
    """Reuse business features; target = is_open (1 open / 0 closed).

    is_open is removed from the features, otherwise the target leaks into the model.
    business.stars was the regression label and is not among the features either.
    """
    df, numeric_cols, categorical_cols = build_business_features(datasets)
    df = df.withColumn("label", F.col("is_open").cast("double")).drop("is_open")
    numeric_cols = [c for c in numeric_cols if c != "is_open"]
    return df, numeric_cols, categorical_cols


def _add_class_weights(train: DataFrame, test: DataFrame):
    """Balance classes: weight(class) = share of the other class in train."""
    share_open = train.agg(F.avg("label")).first()[0]
    weight = F.when(F.col("label") == 1.0, F.lit(1.0 - share_open)).otherwise(F.lit(share_open))
    return train.withColumn("weight", weight), test.withColumn("weight", weight), share_open


def _model_specs() -> list:
    """(name, estimator, needs_scaling, param_grid) for every compared algorithm."""
    lr = LogisticRegression(maxIter=100, weightCol="weight")
    dt = DecisionTreeClassifier(seed=SEED, weightCol="weight")
    rf = RandomForestClassifier(seed=SEED, weightCol="weight")
    gbt = GBTClassifier(seed=SEED, stepSize=0.1, weightCol="weight")

    return [
        ("LogisticRegression", lr, True, (
            ParamGridBuilder()
            .addGrid(lr.regParam, [0.0, 0.01, 0.1])
            .addGrid(lr.elasticNetParam, [0.0, 0.5])
            .build()
        )),
        ("DecisionTreeClassifier", dt, False, (
            ParamGridBuilder()
            .addGrid(dt.maxDepth, [5, 10])
            .addGrid(dt.minInstancesPerNode, [1, 20])
            .build()
        )),
        ("RandomForestClassifier", rf, False, (
            ParamGridBuilder()
            .addGrid(rf.numTrees, [30, 60])
            .addGrid(rf.maxDepth, [8, 12])
            .build()
        )),
        ("GBTClassifier", gbt, False, (
            ParamGridBuilder()
            .addGrid(gbt.maxIter, [30, 60])
            .addGrid(gbt.maxDepth, [3, 5])
            .build()
        )),
    ]


def _evaluate(predictions: DataFrame) -> dict:
    """Metrics are computed without weights, i.e. on the real class distribution."""
    result = {
        m: MulticlassClassificationEvaluator(metricName=m).evaluate(predictions)
        for m in OVERALL_METRICS
    }
    for key, m in CLOSED_METRICS.items():
        result[key] = MulticlassClassificationEvaluator(
            metricName=m, metricLabel=CLOSED,
        ).evaluate(predictions)
    result["auc"] = BinaryClassificationEvaluator(
        rawPredictionCol="rawPrediction", metricName="areaUnderROC",
    ).evaluate(predictions)
    return result


def _confusion(name: str, split: str, predictions: DataFrame) -> list:
    rows = predictions.groupBy("label", "prediction").count().collect()
    return [(name, split, float(r["label"]), float(r["prediction"]), int(r["count"])) for r in rows]


def _weights(model, names: list) -> list:
    """Return (kind, [(feature, weight), ...]) sorted by absolute weight."""
    if isinstance(model, LogisticRegressionModel):
        kind, values = "coefficient", model.coefficients
    else:
        kind, values = "importance", model.featureImportances.toArray()
    pairs = sorted(zip(names, (float(v) for v in values)), key=lambda kv: abs(kv[1]), reverse=True)
    return kind, pairs


def _learning_curve(name: str, best: PipelineModel, train: DataFrame, test: DataFrame) -> list:
    """Log-loss per optimizer step (LR) or per boosting iteration on train and test (GBT)."""
    model = best.stages[-1]
    if isinstance(model, LogisticRegressionModel):
        print(f"   iterations: {model.summary.totalIterations}")
        return [(name, i, "train_objective", float(loss))
                for i, loss in enumerate(model.summary.objectiveHistory)]

    if isinstance(model, GBTClassificationModel):
        features_only = PipelineModel(best.stages[:-1])
        train_losses = model.evaluateEachIteration(features_only.transform(train))
        test_losses = model.evaluateEachIteration(features_only.transform(test))
        rows = []
        for i, (tr, te) in enumerate(zip(train_losses, test_losses), start=1):
            rows.append((name, i, "train_log_loss", float(tr)))
            rows.append((name, i, "test_log_loss", float(te)))
        return rows

    return []


def _metrics_row(name, train_m, test_m, train_time, params) -> tuple:
    return (
        name,
        *(train_m[k] for k in METRIC_KEYS),
        *(test_m[k] for k in METRIC_KEYS),
        train_time,
        params,
    )


def run_classification(spark: SparkSession, datasets: dict, output_dir: str) -> DataFrame:
    """Predict business.is_open with 4 algorithms, tune them with CV and compare on test."""
    print(f"\n{'=' * 60}")
    print(" CLASSIFICATION — predicting business.is_open")
    print(f"{'=' * 60}")

    data, numeric_cols, categorical_cols = build_classification_data(datasets)
    train, test = data.randomSplit([0.8, 0.2], seed=SEED)
    train, test, share_open = _add_class_weights(train, test)
    train, test = train.cache(), test.cache()
    print(f" Train rows: {train.count():,} | Test rows: {test.count():,}")
    print(f" Features: {len(numeric_cols)} numeric + one-hot {categorical_cols}")
    print(f" Class balance (train): open={share_open:.3f}, closed={1 - share_open:.3f}")
    print(f" Class weights: open={1 - share_open:.3f}, closed={share_open:.3f}")

    # baseline: always predict "open" — accuracy equals the share of open businesses
    def baseline_pred(df):
        return df.withColumn("prediction", F.lit(1.0)).withColumn("rawPrediction", F.lit(1.0))

    base_train, base_test = _evaluate(baseline_pred(train)), _evaluate(baseline_pred(test))
    metrics_rows = [_metrics_row("Baseline (always open)", base_train, base_test, 0.0, "prediction=1")]
    confusion_rows = _confusion("Baseline (always open)", "test", baseline_pred(test))
    cv_rows, importance_rows, curve_rows = [], [], []

    for name, estimator, scale, grid in _model_specs():
        print(f"\n--- {name}: {len(grid)} param sets x {NUM_FOLDS} folds ---")
        cv = CrossValidator(
            estimator=Pipeline(stages=feature_stages(numeric_cols, categorical_cols, scale) + [estimator]),
            estimatorParamMaps=grid,
            evaluator=MulticlassClassificationEvaluator(metricName="f1"),
            numFolds=NUM_FOLDS,
            seed=SEED,
            parallelism=2,
        )

        start = time.time()
        cv_model = cv.fit(train)
        train_time = time.time() - start

        for params, avg_f1 in zip(grid, cv_model.avgMetrics):
            cv_rows.append((name, _format_params(params), float(avg_f1)))
            print(f"   CV f1={avg_f1:.4f}  [{_format_params(params)}]")

        best = cv_model.bestModel
        best_params = _format_params(grid[cv_model.avgMetrics.index(max(cv_model.avgMetrics))])
        train_pred, test_pred = best.transform(train), best.transform(test)
        train_m, test_m = _evaluate(train_pred), _evaluate(test_pred)

        print(f"   best: [{best_params}]  fit time {train_time:.1f}s")
        for split, m in (("train", train_m), ("test ", test_m)):
            print(f"   {split} acc={m['accuracy']:.4f} f1={m['f1']:.4f} auc={m['auc']:.4f} | "
                  f"closed: P={m['precision_closed']:.4f} R={m['recall_closed']:.4f} "
                  f"F1={m['f1_closed']:.4f}")
        metrics_rows.append(_metrics_row(name, train_m, test_m, train_time, best_params))

        confusion_rows += _confusion(name, "train", train_pred) + _confusion(name, "test", test_pred)

        # StandardScaler drops slot names, so read them from the assembler output
        names = feature_names(test_pred, "raw_features" if scale else "features")
        kind, weights = _weights(best.stages[-1], names)
        print("   top-10 features:", ", ".join(f"{n}={w:.3f}" for n, w in weights[:10]))
        importance_rows += [(name, n, kind, w) for n, w in weights]

        curve_rows += _learning_curve(name, best, train, test)

    metrics_cols = (
        ["model"]
        + [f"train_{k}" for k in METRIC_KEYS]
        + [f"test_{k}" for k in METRIC_KEYS]
        + ["train_time_sec", "best_params"]
    )
    metrics_df = spark.createDataFrame(metrics_rows, metrics_cols)

    print(f"\n{'=' * 60}")
    print(" CLASSIFICATION — model comparison (test set)")
    print(f"{'=' * 60}")
    shown = ["test_accuracy", "test_f1", "test_auc", "test_precision_closed",
             "test_recall_closed", "test_f1_closed", "train_f1"]
    metrics_df.orderBy(F.desc("test_f1_closed")).select(
        "model", *[F.round(c, 4).alias(c) for c in shown],
    ).show(truncate=False)

    confusion_df = spark.createDataFrame(
        confusion_rows, ["model", "split", "label", "prediction", "count"],
    )
    confusion_df.filter(F.col("split") == "test").orderBy("model", "label", "prediction").show(50)

    _write_csv(metrics_df, f"{output_dir}/classification_metrics")
    _write_csv(spark.createDataFrame(cv_rows, ["model", "params", "cv_avg_f1"]),
               f"{output_dir}/classification_cv_results")
    _write_csv(spark.createDataFrame(importance_rows, ["model", "feature", "kind", "value"]),
               f"{output_dir}/classification_feature_importance")
    _write_csv(confusion_df, f"{output_dir}/classification_confusion_matrix")
    _write_csv(spark.createDataFrame(curve_rows, ["model", "iteration", "series", "value"]),
               f"{output_dir}/classification_learning_curves")

    train.unpersist()
    test.unpersist()
    return metrics_df
