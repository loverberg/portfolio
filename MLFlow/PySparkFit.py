import argparse
import os

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler, StringIndexer, StringIndexerModel
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder
from pyspark.sql import SparkSession
from pyspark.ml.classification import GBTClassifier
import mlflow

os.environ['MLFLOW_S3_ENDPOINT_URL'] = 'https://storage.yandexcloud.net'
os.environ['AWS_ACCESS_KEY_ID'] = 'XXXXXXXXXXXXXXXXXXXXXX'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'

mlflow.set_tracking_uri('https://mlflow.lab.XXXXXXXXXXXXXX')
mlflow.set_experiment(experiment_name='driver_classifications')

LABEL_COL = 'has_car_accident'


def build_pipeline(train_alg):
    """
    Создание пайплаина над выбранной моделью.

    :return: Pipeline
    """
    sex_indexer = StringIndexer(inputCol='sex',
                                outputCol="sex_index")
    car_class_indexer = StringIndexer(inputCol='car_class',
                                      outputCol="car_class_index")
    features = ["age", "sex_index", "car_class_index", "driving_experience",
                "speeding_penalties", "parking_penalties", "total_car_accident"]
    assembler = VectorAssembler(inputCols=features, outputCol='features')
    return Pipeline(stages=[sex_indexer, car_class_indexer, assembler, train_alg])


def evaluate_model(evaluator, predict, metric_list):
    for metric in metric_list:
        evaluator.setMetricName(metric)
        score = evaluator.evaluate(predict)

        mlflow.log_metric(metric, score)
        print(f"{metric} score = {score}")

def optimization(pipeline, gbt, train_df, evaluator):
    grid = ParamGridBuilder() \
        .addGrid(gbt.maxDepth, [3, 5]) \
        .addGrid(gbt.maxIter, [20, 30]) \
        .addGrid(gbt.maxBins, [16, 32]) \
        .build()
    tvs = TrainValidationSplit(estimator=pipeline,
                               estimatorParamMaps=grid,
                               evaluator=evaluator,
                               trainRatio=0.8)
    models = tvs.fit(train_df)
    return models.bestModel

def mlflow_log(model):
    mv = mlflow.spark.log_model(model,
                                artifact_path='driver_classifications',
                                registered_model_name='driver_classifications')
    return mv


def process(spark, train_path, test_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param train_path: путь до тренировочного датасета
    :param test_path: путь до тренировочного датасета
    """
    mlflow.start_run()

    evaluator = MulticlassClassificationEvaluator(labelCol=LABEL_COL, predictionCol="prediction", metricName='f1')
    train_df = spark.read.parquet(train_path)
    test_df = spark.read.parquet(test_path)

    gbt = GBTClassifier(labelCol=LABEL_COL)
    pipeline = build_pipeline(gbt)

    model = optimization(pipeline, gbt, train_df, evaluator)
    predict = model.transform(test_df)

    evaluate_model(evaluator, predict, ['f1', 'weightedPrecision', 'weightedRecall', 'accuracy'])
    print('Best model saved')

    mlflow.log_param('input_columns', train_df.columns)
    mlflow.log_param('maxDepth', model.stages[-1].getMaxDepth())
    mlflow.log_param('maxIter', model.stages[-1].getMaxIter())
    mlflow.log_param('maxBins', model.stages[-1].getMaxBins())
    mlflow.log_param('target', model.stages[-1].getLabelCol())
    mlflow.log_param('features ', model.stages[2].getInputCols())
    mlflow.log_param('stage_0', model.stages[0])
    mlflow.log_param('stage_1', model.stages[1])
    mlflow.log_param('stage_2', model.stages[2])
    mlflow.log_param('stage_3', model.stages[3])

    mlflow_log(model)
    mlflow.end_run()

def main(train_path, test_path):
    spark = _spark_session()
    process(spark, train_path, test_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkMLJob').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--train', type=str, default='data/train.parquet', help='Please set train datasets path.')
    parser.add_argument('--test', type=str, default='data/test.parquet', help='Please set test datasets path.')
    args = parser.parse_args()
    train = args.train
    test = args.test
    main(train, test)
