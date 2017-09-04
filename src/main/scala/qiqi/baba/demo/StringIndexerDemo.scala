package qiqi.baba.demo

/**
  * Created by lhd on 8/16/17.
  */
object StringIndexerDemo extends App with BasicClass{
  import org.apache.spark.ml.feature.StringIndexer

  val df = spark.createDataFrame(
    Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
  ).toDF("id", "category")

  val indexer = new StringIndexer()
    .setInputCol("category")
    .setOutputCol("categoryIndex")

  val indexed = indexer.fit(df).transform(df)
  indexed.show()
}
