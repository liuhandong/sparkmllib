package qiqi.baba.demo


import org.apache.spark.SparkConf
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.kafka010.KafkaUtils

import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/**
  * Created by lhd on 8/2/17.
  */
object KafkaStreaming10Demo {
  // 0.10.0sample
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[20]").setAppName("kafka-spark-demo")
    val scc = new StreamingContext(sparkConf, Duration(5000))
    //scc.checkpoint(".") // 因为使用到了updateStateByKey,所以必须要设置checkpoint
    //val topics = Set("apptest") //我们需要消费的kafka数据的topic
    val topics = Array("apptest")
    val kafkaParams = Map[String, Object](
      //"metadata.broker.list" -> "localhost:9092" // kafka的broker list地址
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream"
    )

    val stream = KafkaUtils.createDirectStream(scc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    stream
      .map(_.value())
      .flatMap(_.split(" "))
      .map(r => (r, 1))
      .reduceByKey(_ + _)
      .print
    //    stream.map(_._2)      // 取出value
    //      .flatMap(_.split(" ")) // 将字符串使用空格分隔
    //      .map(r => (r, 1))      // 每个单词映射成一个pair
    //      //.updateStateByKey[Int](updateFunc)  // 用当前batch的数据区更新已有的数据
    //      .reduceByKey(_+_)
    //      .print() // 打印前10个数据

    scc.start() // 真正启动程序
    scc.awaitTermination() //阻塞等待
  }

  /**
    *
    */
  val updateFunc = (currentValues: Seq[Int], preValue: Option[Int]) => {
    val curr = currentValues.sum
    val pre = preValue.getOrElse(0)
    Some(curr + pre)
  }
}
