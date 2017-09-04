package qiqi.baba.demo

import org.apache.spark.sql.SparkSession

/**
  * Created by lhd on 8/9/17.
  */
object CountVectorizerDemo extends App with BasicClass{
  import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
//  val spark = SparkSession.builder()
//    .master("local[*]")
//    .appName("features app")
//    .getOrCreate()
  val df = spark.createDataFrame(Seq(
    (0, Array("a", "b", "c")),
    //(1, Array("a", "b", "b", "c", "a"))
    (1, Array("a", "b", "c", "a"))
  )).toDF("id", "words")

  // fit a CountVectorizerModel from the corpus
  val cvModel: CountVectorizerModel = new CountVectorizer()
    .setInputCol("words")
    .setOutputCol("features")
    .setVocabSize(3)
    .setMinDF(2)
    .fit(df)

  // alternatively, define CountVectorizerModel with a-priori vocabulary
  val cvm = new CountVectorizerModel(Array("a", "b", "c"))
    .setInputCol("words")
    .setOutputCol("features")

  cvModel.transform(df).show()
}
