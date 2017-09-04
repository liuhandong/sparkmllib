package qiqi.baba.demo

/**
  * Created by lhd on 8/16/17.
  */
object OneHotEncoderDemo extends App with BasicClass{
  import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}

  val df = spark.createDataFrame(Seq(
    (0, "a"),
    (1, "b"),
    (2, "c"),
    (3, "a"),
    (4, "a"),
    (5, "c")
  )).toDF("id", "category")

  val indexer = new StringIndexer()
    .setInputCol("category")
    .setOutputCol("categoryIndex")
    .fit(df)
  val indexed = indexer.transform(df)

  val encoder = new OneHotEncoder()
    .setInputCol("categoryIndex")
    .setOutputCol("categoryVec")

  val encoded = encoder.transform(indexed)
  encoded.show()
}
