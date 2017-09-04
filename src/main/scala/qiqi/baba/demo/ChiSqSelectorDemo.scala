package qiqi.baba.demo

/**
  * Created by lhd on 8/16/17.
  *
  * chisqselector代表卡方特征选择。它通过标记数据分类特征。C
  * hiSqSelector用卡方独立性检验来决定哪些功能选择。
  * 它支持五种选择方法：numtopfeatures，percentile，FPR，fdr，fwe：
  * *numtopfeatures选择固定数量的顶级功能根据卡方检验。这类似于产生具有最大预测能力的特性。
  * *percentile相似，但选择numtopfeatures所有特征的一部分而不是一个固定的数。
  * *FPR对所有特征的选择值低于阈值，从而控制选择的假阳性率。
  * *fdr采用Benjamini Hochberg程序选择所有特征的错误发现率低于一个阈值。
  * *我们选择所有功能的P值低于阈值。门槛是1 / numfeatures缩放，从而控制选择家庭明智的错误率。
  * 默认情况下，选择的方法是numtopfeatures，与顶部的功能设置为50的默认值。
  * 用户可以选择使用setselectortype选择方法。
  */
object ChiSqSelectorDemo extends App with BasicClass{
  import org.apache.spark.ml.feature.ChiSqSelector
  import org.apache.spark.ml.linalg.Vectors

  val data = Seq(
    (7, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1.0),
    (8, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0.0),
    (9, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0.0)
  )

  //val df = spark.createDataset(data).toDF("id", "features", "clicked")
  val df = spark.createDataFrame(data).toDF("id", "features", "clicked")

  val selector = new ChiSqSelector()
    .setNumTopFeatures(1)
    .setFeaturesCol("features")
    .setLabelCol("clicked")
    .setOutputCol("selectedFeatures")

  val result = selector.fit(df).transform(df)

  println(s"ChiSqSelector output with top ${selector.getNumTopFeatures} features selected")
  result.show()
}
