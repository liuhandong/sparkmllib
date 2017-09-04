package qiqi.baba.demo

/**
  * Created by lhd on 8/16/17.
  */
object SQLTransformerDemo extends App with BasicClass{
  import org.apache.spark.ml.feature.SQLTransformer

  val df = spark.createDataFrame(
    Seq((0, 1.0, 3.0), (2, 2.0, 5.0))).toDF("id", "v1", "v2")

  val sqlTrans = new SQLTransformer().setStatement(
    "SELECT *, (v1 + v2) AS v3, (v1 * v2) AS v4 FROM __THIS__")

  sqlTrans.transform(df).show()
}
