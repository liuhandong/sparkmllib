package qiqi.baba.demo

/**
  * Created by lhd on 8/16/17.
  * Bucketizer把一列一列连续的功能特征的桶，在桶是由用户指定。
  * 它需要一个参数：拆分：将连续特征映射为桶的参数。n + 1的分裂，有N个水桶。
  * 通过除x、y所定义的桶，除了最后一个桶外，还保留了取值范围为x、y的值。必须显式地提供INF、INF值以覆盖所有双值；
  * 否则，指定的拆分之外的值将被视为错误。分裂的两个例子是阵列（double.negativeinfinity，0，1，double。
  * positiveinfinity）和数组（0，1，2）。
  * 注意，如果你不知道的目标列的上下界，你应该添加double.negativeinfinity和double.positiveinfinity作为
  * 你将以防止潜在的bucketizer边界异常的界限。请注意，您所提供的分割必须严格递增，即S0 < S1 <…< Sn
  * 更多的细节可以在API文档中找到bucketizer。
  */
object BucketizerDemo extends App with BasicClass{
  import org.apache.spark.ml.feature.Bucketizer

  val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity)

  val data = Array(-999.9, -0.5, -0.3, 0.0, 0.2, 999.9)
  val dataFrame = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

  val bucketizer = new Bucketizer()
    .setInputCol("features")
    .setOutputCol("bucketedFeatures")
    .setSplits(splits)

  // Transform original data into its bucket index.
  val bucketedData = bucketizer.transform(dataFrame)

  println(s"Bucketizer output with ${bucketizer.getSplits.length-1} buckets")
  bucketedData.show()
}
