package qiqi.baba.demo

import org.apache.spark.sql.SparkSession

/**
  * Created by lhd on 8/8/17.
  */
object TF_IDF_FeatureExtractors extends App with BasicClass{
  //import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
//  val spark = SparkSession.builder()
//    .appName("some examples")
//    .master("local[*]")
//    .appName("features app")
//    .getOrCreate()
//  val sentenceData = spark.createDataFrame(Seq(
//    (0.0, "Hi I heard about Spark"),
//    (0.0, "I wish Java could use case classes"),
//    (1.0, "Logistic regression models are neat")
//  )).toDF("label", "sentence")
//
//  val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
//  val wordsData = tokenizer.transform(sentenceData)
//
//  val hashingTF = new HashingTF()
//    .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
//
//  val featurizedData = hashingTF.transform(wordsData)
//  // alternatively, CountVectorizer can also be used to get term frequency vectors
//
//  val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
//  val idfModel = idf.fit(featurizedData)
//
//  val rescaledData = idfModel.transform(featurizedData)
//  rescaledData.select("label", "features")
//  .show()



//  val sentenceData = spark.createDataFrame(Seq(
//    (0, "Hi I heard about Spark"),
//    (0, "I wish Java could use case classes"),
//    (1, "Logistic regression models are neat")
//  )).toDF("label", "sentence")
//
//  val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
//
//  val wordsData = tokenizer.transform(sentenceData)
//  wordsData.select("label","words").foreach(s=>print (","+s))
//  val hashingTF = new HashingTF()
//    .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
//  println("====================================")
//
//  val featurizedData = hashingTF.transform(wordsData)
//  // CountVectorizer也可获取词频向量
//  featurizedData.select("label","rawFeatures").foreach(s=>print (","+s))
//  val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
//  println("====================================")
//
//  val idfModel = idf.fit(featurizedData)
//  val rescaledData = idfModel.transform(featurizedData)
//  rescaledData.select("label","features" ).take(3).foreach(println)


  import org.apache.spark.ml.feature.Word2Vec
  import org.apache.spark.ml.linalg.Vector
  import org.apache.spark.sql.Row

  // Input data: Each row is a bag of words from a sentence or document.
  val documentDF = spark.createDataFrame(Seq(
    "Hi I heard about Spark".split(" "),
    "I wish Java could use case classes".split(" "),
    "Logistic regression models are neat".split(" ")
  ).map(Tuple1.apply)).toDF("text")
  documentDF.select("text").foreach(f=>print(","+f))
  println
  // Learn a mapping from words to Vectors.
  val word2Vec = new Word2Vec()
    .setInputCol("text")
    .setOutputCol("result")
    .setVectorSize(3)
    .setMinCount(0)
  val model = word2Vec.fit(documentDF)

  val result = model.transform(documentDF)
  result.collect().foreach { case Row(text: Seq[_], features: Vector) =>
    println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n") }
}
