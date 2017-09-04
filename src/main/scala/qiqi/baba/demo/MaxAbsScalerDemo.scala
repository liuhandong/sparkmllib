package qiqi.baba.demo

/**
  * Created by lhd on 8/16/17.
  * maxabsscaler变换矢量数据集，
  * 每个重标度特征的范围（1，1）通过dividing通最大绝对值在每个特征。
  * 它并不SHIFT键/数据中心，因此并不destroy any的稀疏性。
  * maxabsscaler computes汇总统计的数据集和产生一个maxabsscalermodel。
  * 该模型可以变换，然后对每个特征的individually范围[ 1 ]。
  */
object MaxAbsScalerDemo extends App with BasicClass{
  import org.apache.spark.ml.feature.MaxAbsScaler
  import org.apache.spark.ml.linalg.Vectors

  val dataFrame = spark.createDataFrame(Seq(
    (0, Vectors.dense(1.0, 0.1, -8.0)),
    (1, Vectors.dense(2.0, 1.0, -4.0)),
    (2, Vectors.dense(4.0, 10.0, 8.0))
  )).toDF("id", "features")

  val scaler = new MaxAbsScaler()
    .setInputCol("features")
    .setOutputCol("scaledFeatures")

  // Compute summary statistics and generate MaxAbsScalerModel
  val scalerModel = scaler.fit(dataFrame)

  // rescale each feature to range [-1, 1]
  val scaledData = scalerModel.transform(dataFrame)
  scaledData.select("features", "scaledFeatures").show()
}
