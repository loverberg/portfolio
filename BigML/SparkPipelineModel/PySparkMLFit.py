import operator
import argparse

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql import SparkSession
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import GBTClassifier

MODEL_PATH = 'spark_ml_model'
LABEL_COL = 'is_bot'

def process(spark, data_path, model_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param data_path: путь до датасета
    :param model_path: путь сохранения обученной модели
    """
    session_stat_df = spark.read.parquet(data_path)

    indexer_user_type = StringIndexer(inputCol='user_type', outputCol="user_type_index")
    indexer_platform = StringIndexer(inputCol='platform', outputCol="platform_index")

    feature = VectorAssembler(
        inputCols=['duration', 'item_info_events', 'select_item_events', 'make_order_events', 'events_per_min',
                   'user_type_index', 'platform_index'],
        outputCol="features")

    dt_classifier = DecisionTreeClassifier(labelCol=LABEL_COL, featuresCol="features")
    rf_classifier = RandomForestClassifier(labelCol=LABEL_COL, featuresCol="features")
    gbtc_classifier = GBTClassifier(labelCol=LABEL_COL, featuresCol="features")

    dt_pipeline = Pipeline(stages=[indexer_user_type, indexer_platform, feature, dt_classifier])
    rf_pipeline = Pipeline(stages=[indexer_user_type, indexer_platform, feature, rf_classifier])
    gbtc_pipeline = Pipeline(stages=[indexer_user_type, indexer_platform, feature, gbtc_classifier])


    dt_paramGrid = ParamGridBuilder() \
        .addGrid(dt_classifier.maxDepth, [3, 4]) \
        .addGrid(dt_classifier.maxBins, [6, 12]) \
        .addGrid(dt_classifier.minInfoGain, [0.05, 0.1]) \
        .build()

    rf_paramGrid = ParamGridBuilder() \
        .addGrid(rf_classifier.maxDepth, [2, 3])\
        .addGrid(rf_classifier.maxBins, [4, 5])\
        .addGrid(rf_classifier.minInfoGain, [0.1, 0.15]) \
        .build()

    gbtc_paramGrid = ParamGridBuilder() \
        .addGrid(gbtc_classifier.maxDepth, [2, 4]) \
        .addGrid(gbtc_classifier.stepSize, [0.1, 0.2]) \
        .build()

    evaluator = MulticlassClassificationEvaluator(
        labelCol=LABEL_COL,
        predictionCol="prediction",
        metricName="f1")

    dt_cv = CrossValidator(
        estimator=dt_pipeline,
        estimatorParamMaps=dt_paramGrid,
        evaluator=evaluator,
        parallelism=2)

    rf_cv = CrossValidator(
        estimator=rf_pipeline,
        estimatorParamMaps=rf_paramGrid,
        evaluator=evaluator,
        parallelism=2)

    gbtc_cv = CrossValidator(
        estimator=gbtc_pipeline,
        estimatorParamMaps=gbtc_paramGrid,
        evaluator=evaluator,
        parallelism=2)

    dt_model = dt_cv.fit(session_stat_df)
    rf_model = rf_cv.fit(session_stat_df)
    gbtc_model = gbtc_cv.fit(session_stat_df)

    dt_prediction = dt_model.transform(session_stat_df)
    rf_prediction = rf_model.transform(session_stat_df)
    gbtc_prediction = gbtc_model.transform(session_stat_df)

    f1_measure = {dt_model: evaluator.evaluate(dt_prediction),
                  rf_model: evaluator.evaluate(rf_prediction),
                  gbtc_model: evaluator.evaluate(gbtc_prediction)}

    for model in f1_measure.items():
        if model[0] == max(f1_measure.items(), key=operator.itemgetter(1))[0]:
            print('F1 Score: {}'.format(max(f1_measure.items(), key=operator.itemgetter(1))[0]))
            best_model = model[0].bestModel

            best_model.write().overwrite().save(model_path)
            print(type(best_model))

            jo = model[0].bestModel.stages[-1]._java_obj
            print('Max Depth: {}'.format(jo.getMaxDepth()))
            print('Num Trees: {}'.format(jo.getMaxBins()))
            print('Impurity: {}'.format(jo.getMinInfoGain()))



def main(data_path, model_path):
    spark = _spark_session()
    process(spark, data_path, model_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkMLFitJob').getOrCreate()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_path', type=str, default='session-stat.parquet', help='Please set datasets path.')
    parser.add_argument('--model_path', type=str, default=MODEL_PATH, help='Please set model path.')
    args = parser.parse_args()
    data_path = args.data_path
    model_path = args.model_path
    main(data_path, model_path)
