from __future__ import print_function

from pyspark.ml.regression import DecisionTreeRegressor

from pyspark.sql import SparkSession
from pyspark.ml.linalg import VectorAssembler

if name __name__ == "__main__":

    # Create a sparkSession
    spark = SparkSession.builder.appName("decisionTreesInSparkML").getOrCreate()

    # Load up data as dataframe
    data = spark.read.option("header", "true").option("inferSchema", "true")\
        .csv("file:///SparkCourse/realestate.csv")

    assembler = VectorAssembler().setInputCols(["HouseAge", "DistanceToMRT",\
                                "NumberConvenienceStores"]).setOutputCol("features")

    # split data into training data and testing data
    trainTest = df.randomSplit([0.8, 0.2])
    trainingDF = trainTest[0]
    testDF = trainTest[1]

    df = assembler.transform(data).select("PriceOfUnitArea", "features")

    # Create a decission trees regressor model
    dtr = DecisionTreeRegressor().setFeaturesCol("features").setLabelCol("PriceOfUnitArea")

    # Train the model
    model = dtr.fit(trainingDF)

    # Predict values in test data
    fullPredictions = model.tranform(testDF).ceche()

    # Extract the predictions
    predictions = fullPredictions.select("prediction").rdd.map(lambda x: x[0])
    labels = fullPredictions.select("PriceOfUnitArea").rdd.map(lambda x: x[0])

    # Zip them together
    predictionsAndLabel = predictions.zip(labels).collect()

    # Print out
    for pred in predictionsAndLabel:
        print(pred)

    # Stop the session
    spark.stop()