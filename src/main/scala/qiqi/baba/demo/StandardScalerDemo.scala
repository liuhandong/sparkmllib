package qiqi.baba.demo

/**
  * Created by lhd on 8/16/17.
  */
object StandardScalerDemo extends App with BasicClass{
  import org.apache.spark.ml.feature.StandardScaler

  val dataFrame = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

  val scaler = new StandardScaler()
    .setInputCol("features")
    .setOutputCol("scaledFeatures")
    .setWithStd(true)
    .setWithMean(false)

  // Compute summary statistics by fitting the StandardScaler.
  val scalerModel = scaler.fit(dataFrame)

  // Normalize each feature to have unit standard deviation.
  val scaledData = scalerModel.transform(dataFrame)
  scaledData.show(false)
}
