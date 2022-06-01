import argparse
import os


from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import mlflow
from mlflow.tracking import MlflowClient

os.environ['MLFLOW_S3_ENDPOINT_URL'] = 'https://storage.yandexcloud.net'
os.environ['AWS_ACCESS_KEY_ID'] = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'

mlflow.set_tracking_uri("https://mlflow.lab.XXXXXXXXXXXXXXXXXX")
client = MlflowClient()

NAME_EXP = 'driver_classifications'


def get_last_prod_model(name):
    """
    Обращение к артефактам MLFlow
    :param name: имя эксперимента
    :return: model version
    """
    last_models = client.get_registered_model(name).latest_versions
    for model in last_models:
        if model.current_stage == 'Production':
            return model
        else:
            pass

def load_model():
    """
    Загрузка модели
    :return: production model
    """
    model_version = get_last_prod_model(NAME_EXP)
    model = mlflow.spark.load_model(f'models:/{NAME_EXP}/{model_version.version}')

    print('Loaded:', type(model))
    return model


def process(spark, data_path, result):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param data_path: путь до датасета
    :param result: путь сохранения результата
    """
    data = spark.read.parquet(data_path)
    model = load_model()
    prediction = model.transform(data)
    prediction.write.mode("overwrite").parquet(result)


def main(data, result):
    spark = _spark_session()
    process(spark, data, result)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkPredict').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', type=str, default='data.parquet', help='Please set datasets path.')
    parser.add_argument('--result', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    data = args.data
    result = args.result
    main(data, result)
