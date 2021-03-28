package com.github.jobs

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * This class performs a Spark ML Logistical Regression calculation
 * against Iris flower data - to classify the type of Iris
 * flower based on its features. The independent variables, denoting
 * the features of the plants, are continuous rather than binary
 * variables. Also, the possible categories of Iris plants exceeds two, so
 * the type of logical regression model required is 'multinomial' -
 * the three dependant variables are: Iris-setosa, Iris-versicolor
 * and Iris-virginica.
 */
object IrisPlantSpeciesCategorization {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Iris - LogisticRegression")
      .master("local[1]")
      .getOrCreate()

    val schema = StructType(
      StructField("sepal_length", DoubleType, nullable = false) ::
        StructField("sepal_width", DoubleType, nullable = false) ::
        StructField("petal_length", DoubleType, nullable = false) ::
        StructField("petal_width", DoubleType, nullable = false) ::
        StructField("class", StringType, nullable = false) ::
        Nil
    )

    val irisDF = spark.read.format("csv")
      .option("header", value = true)
      .option("delimiter", ",")
      .option("mode", "DROPMALFORMED")
      .schema(schema)
      .load("resources/iris.csv")
      .cache()

    runMLLogisticRegression(irisDF)
  }

  /**
   * This method performs a logistical regression
   * on the Iris dataset. There are four keys stages
   * to running this regression:
   *
   * 1) The vectors which are to be used by the
   * regression model have to be explicitly declared.
   *
   * 2) The classification column, which denotes
   * the type of Iris plant screened in the training
   * model, needs to be explicitly declared.
   *
   * 3) The logistical regression model created then
   * needs to be fitted with the training data and
   * transformed to devise statistics on the accuracy
   * of the data model.
   *
   * 4) Finally this regression model can be used
   * to declare the type of Iris plant based on the
   * features ran against the model.
   *
   * @param irisDF the iris dataset as a spark dataframe
   */
  def runMLLogisticRegression(irisDF: DataFrame) {
    //Create a features column so that it can be used in the ML algorithm
    val cols = Array("sepal_length", "sepal_width", "petal_length", "petal_width")
    val assembler = new VectorAssembler()
      .setInputCols(cols)
      .setOutputCol("features")
    val featureDf = assembler.transform(irisDF)

    featureDf.printSchema()
    featureDf.foreach(row => println(row))

    //Set the classification column for the Multinomial logistical regression calculation
    val indexer = new StringIndexer()
      .setInputCol("class")
      .setOutputCol("label")
    val labelDf = indexer.fit(featureDf).transform(featureDf)

    labelDf.foreach(row => println(row))
    labelDf.printSchema()

    val seed = 5043
    val Array(trainingData, testData) = labelDf.randomSplit(Array(0.7, 0.3), seed)

    // train logistic regression model with training data set
    val logisticRegression = new LogisticRegression()
      .setFitIntercept(true)
      .setMaxIter(100)
      .setRegParam(0.02)
      .setElasticNetParam(0.8)
    val logisticRegressionModel = logisticRegression.fit(trainingData)

    // run model with test data set to get predictions
    // this will add new columns rawPrediction, probability and prediction
    val predictionDf = logisticRegressionModel.transform(testData)
    predictionDf.show(10, false)
  }
}
