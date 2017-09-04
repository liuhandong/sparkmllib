package qiqi.baba.demo

import org.apache.spark.sql.SparkSession

/**
  * Created by lhd on 8/9/17.
  */
object TokenizerDemo extends  App with BasicClass{
  import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
  import org.apache.spark.sql.functions._
//  val spark = SparkSession.builder()
//    .master("local[*]")
//    .appName("features app")
//    .getOrCreate()
  val sentenceDataFrame = spark.createDataFrame(Seq(
    (0, "Hi I heard about Spark"),
    (1, "I wish Java could use case classes"),
    (2, "Logistic,regression,models,are,neat")
  )).toDF("id", "sentence")

  val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
  val regexTokenizer = new RegexTokenizer()
    .setInputCol("sentence")
    .setOutputCol("words")
    .setPattern("\\W") // alternatively .setPattern("\\w+").setGaps(false)

  val countTokens = udf { (words: Seq[String]) => words.length }

  val tokenized = tokenizer.transform(sentenceDataFrame)
  tokenized.select("sentence", "words")
    .withColumn("tokens", countTokens(col("words"))).show(false)

  val regexTokenized = regexTokenizer.transform(sentenceDataFrame)
  regexTokenized.select("sentence", "words")
    .withColumn("tokens", countTokens(col("words"))).show(false)
}
