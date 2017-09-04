package qiqi.baba.demo

import org.apache.spark.sql.SparkSession

/**
  * Created by lhd on 8/14/17.
  */
trait BasicClass {
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("features app")
    .getOrCreate()

}
