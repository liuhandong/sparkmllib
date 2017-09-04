package qiqi.baba.demo

/**
 * Hello world!
 *
 */
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.{Seconds,StreamingContext}
//import org.apache.spark.streaming.kafka010.KafkaUtils
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming.kafka.KafkaUtils
//import StorageLevel._
//import for an implicit conversion:
//import org.apache.spark._
//import org.apache.spark.streaming.Minutes
//import org.apache.spark.streaming.StreamingContext._

object KafkaStreamingDemo  {
  def main(args: Array[String]): Unit = {//0.8.0 sample

//    val sc = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming Example")
//
//    val ssc = new StreamingContext(sc, Seconds(2))
//    val zkQuorum = "localhost:2181"
//    val group = "test-group"
//    val topics = "apptest"
//    val numThreads = 1
//    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
//    val lineMap = KafkaUtils.createStream(ssc, zkQuorum, group,topicMap)
//    val lines = lineMap.map(_._2)
//    val words = lines.flatMap(_.split(" "))
//    //11.Create the key-value pair of (word,occurrence):
//    val pair = words.map( x => (x,1))
//    // 12. Do the word count for a sliding window:
//    val wordCounts = pair.reduceByKey(_+_)//pair.reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
//    wordCounts.print
//    //Set the checkpoint directory:
//    //ssc.checkpoint("hdfs://localhost:9000/user/hduser/checkpoint")
//    //Start StreamingContext:
//    ssc.start
//    ssc.awaitTermination
/*
    val sc = new SparkConf().setMaster("local[3]").setAppName("SparkStreaming Example")
    //Create StreamingContext with a 2 second batch interval:
    val ssc = new StreamingContext(sc, Seconds(1))

    //Create a SocketTextStream Dstream on localhost with port 8585 with the
    //MEMORY_ONLY caching:
    val lines = ssc.socketTextStream("localhost",9999,MEMORY_ONLY)
    //Divide the lines into multiple words:
    val wordsFlatMap = lines.flatMap(_.split(" "))
    //Convert word to (word,1), that is, output 1 as the value for each occurrence of a word
    //as the key:
    val wordsMap = wordsFlatMap.map( w => (w,1))
    //Use the reduceByKey method to add a number of occurrences for each word as the
    //key (the function works on two consecutive values at a time, represented by a and b):
    val wordCount = wordsMap.reduceByKey((a,b)=>(a+b))
    //val wordCount = wordsMap.reduceByKey( _+_)//(a,b) => (a+b)
    //Print wordCount:
    wordCount.print
    //Start StreamingContext; remember, nothing happens until StreamingContext
    //is started:
    ssc.start
    ssc.awaitTermination()
*/
    /*
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)
    // Split each line into words
    val words = lines.flatMap(_.split(" "))
    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
    */
  }
}
