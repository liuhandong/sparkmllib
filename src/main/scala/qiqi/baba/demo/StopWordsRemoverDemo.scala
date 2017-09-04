package qiqi.baba.demo

import org.apache.spark.sql.SparkSession

/**
  * Created by lhd on 8/9/17.
  */
object StopWordsRemoverDemo extends App with BasicClass{
//  val spark = SparkSession.builder()
//    .master("local[*]")
//    .appName("features app")
//    .getOrCreate()
  import org.apache.spark.ml.feature.StopWordsRemover
  val remover = new StopWordsRemover()
    .setInputCol("raw")
    .setOutputCol("filtered")

  val dataSet = spark.createDataFrame(Seq(
    (0, Seq("I", "saw", "the", "red", "balloon")),
    (1, Seq("Mary", "had", "a", "little", "lamb"))
  )).toDF("id", "raw")

  remover.transform(dataSet).show(false)
}
