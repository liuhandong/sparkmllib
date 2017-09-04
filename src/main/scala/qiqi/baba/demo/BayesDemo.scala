package qiqi.baba.demo

/**
  * Created by lhd on 8/8/17.
  */

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SparkSession

object BayesDemo {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[90]").setAppName("ml lib")


    val spark = SparkSession.builder()
      .appName("Spark SQL basic example")
      //.config("spark.some.config.option", "some-value")
      .config(sc)
      .getOrCreate()
    // Load the data stored in LIBSVM format as a DataFrame.
    val data = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    // Split the data into training and test sets (30% held out for testing)
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3), seed = 1234L)

    // Train a NaiveBayes model.
    val model = new NaiveBayes()
      .fit(trainingData)

    // Select example rows to display.
    val predictions = model.transform(testData)
    predictions.show()

    // Select (prediction, true label) and compute test error
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Accuracy: " + accuracy)
  }

}
