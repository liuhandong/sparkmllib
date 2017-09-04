package qiqi.baba.demo

import org.apache.spark.sql.SparkSession

/**
  * Created by lhd on 8/8/17.
  */
object RFormulaDemo extends App{
  import org.apache.spark.ml.feature.RFormula
  val spark = SparkSession.builder()
    .appName("Spark SQL basic example")
    .master("local[40]")
    .appName("features app")
    .getOrCreate()
  val dataset = spark.createDataFrame(Seq(
    (7, "US", 18, 1.0),
    (8, "CA", 12, 0.0),
    (9, "NZ", 15, 0.0)
  )).toDF("id", "country", "hour", "clicked")
  val formula = new RFormula()
    .setFormula("clicked ~ country + hour")
    .setFeaturesCol("features")
    .setLabelCol("label")
  val output = formula.fit(dataset).transform(dataset)
  output.select("features", "label").show()
}
