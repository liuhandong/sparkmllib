package qiqi.baba.demo

import org.apache.spark.sql.SparkSession

/**
  * Created by lhd on 8/14/17.
  * 语言是一个序列的[数学] N标记（通常是加工误差的话）一些整数[数学] n.
  * 加工误差的Ngram类可以用来变换输入特征为[数学]实例加工误差。
  * NGram需要输入一个字符串序列（如一个标记输出）。
  * 参数n是用来确定每个[数学] n-gram加工误差项的数目。
  * 输出将包括一系列的[数学]，
  * [错误处理实例中每个数学处理错误]的n-gram是由一个空间分隔的字符串表示的[数学]连续N字处理错误。
  * 如果输入序列包含少于n个字符串，则不生成输出。
  */
object N_gramDemo extends App with BasicClass{
//  val spark = SparkSession.builder()
//    .master("local[*]")
//    .appName("features app")
//    .getOrCreate()
  import org.apache.spark.ml.feature.NGram

  val wordDataFrame = spark.createDataFrame(Seq(
    (0, Array("Hi", "I", "heard", "about", "Spark")),
    (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
    (2, Array("Logistic", "regression", "models", "are", "neat"))
  )).toDF("id", "words")

  val ngram = new NGram().setN(2).setInputCol("words").setOutputCol("ngrams")

  val ngramDataFrame = ngram.transform(wordDataFrame)
  ngramDataFrame/*.select("id","ngrams")*/.show(false)
}
