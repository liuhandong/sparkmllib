package qiqi.baba.demo

/**
  * Created by lhd on 8/16/17.
  * QuantileDiscretizer以一列的连续特征和输出的分级分类的功能列。
  * 箱的数目是由numbuckets参数。
  * 这是可能的，水桶的使用数量会小于这个值，如果有太少的不同的值的输入创造足够的不同的分位数。
  * NaN值：NaN值将在quantilediscretizer拟合列删除。这将产生一个bucketizer模型预测。
  * 在转型过程中，bucketizer将引发错误，当它发现南值数据集，但用户也可以选择保留或删除的NaN值在数据集内设置handleinvalid。
  * 如果用户选择继续南的价值，他们将被特殊处理，放进自己的桶，
  * 例如，如果使用4桶，然后非南数据将桶[ 0-3 ]，但奶奶会算在一个特殊的斗[ 4 ]。
  * 算法：本范围的选择使用近似算法（见详细描述了approxquantile文档）。
  * 近似的精度可与相对误差参数控制。当设置为零，计算精确的位数（注：计算精确的位数是一个昂贵的操作）。
  * 下和上bin界限将是-无限和+无限覆盖所有实际值。
  */
object QuantileDiscretizerDemo extends App with BasicClass{
  import org.apache.spark.ml.feature.QuantileDiscretizer

  val data = Array((0, 18.0), (1, 19.0), (2, 8.0), (3, 5.0), (4, 2.2))
  val df = spark.createDataFrame(data).toDF("id", "hour")

  val discretizer = new QuantileDiscretizer()
    .setInputCol("hour")
    .setOutputCol("result")
    .setNumBuckets(3)

  val result = discretizer.fit(df).transform(df)
  result.show(false)
}
