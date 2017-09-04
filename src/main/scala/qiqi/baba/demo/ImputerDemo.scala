package qiqi.baba.demo

/**
  * Created by lhd on 8/16/17.
  * Imputer转换完成数据集合中的缺失值，使用平均值或列中的缺失值位于正中。
  * 输入列应该是double或浮子式。目前Imputer不支持分类的特征和可能造成包含不正确分类特征列的值。
  * 注意，输入列中的所有空值都被视为丢失，因此也被计算。
  */
object ImputerDemo extends App with BasicClass{
  import org.apache.spark.ml.feature.Imputer

  val df = spark.createDataFrame(Seq(
    (1.0, Double.NaN),
    (2.0, Double.NaN),
    (Double.NaN, 3.0),
    (4.0, 4.0),
    (5.0, 5.0)
  )).toDF("a", "b")

  val imputer = new Imputer()
    .setInputCols(Array("a", "b"))
    .setOutputCols(Array("out_a", "out_b"))

  val model = imputer.fit(df)
  model.transform(df).show()
}
