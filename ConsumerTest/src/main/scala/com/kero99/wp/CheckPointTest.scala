package com.kero99.wp

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  *
  * @author wp
  * @date 2019-09-05 15:57
  *
  */
object CheckPointTest extends App {
  val cfg = new SparkConf().setMaster("local[2]").setAppName("wc")
  val ssc = new StreamingContext(cfg,Seconds(5))
  ssc.sparkContext.setLogLevel("WARN")
  ssc.checkpoint("hdfs://apache01:9000/wc_cp")

  val kafkaParams=Map("bootstrap.servers"->"apache01:9092,apache02:9092,apache03:9092")

  val topics = Set("wp")
  val dStream=KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)

  dStream.flatMap(_._2.split(" ")).filter(!_.isEmpty).map((_,1)).updateStateByKey((values:Seq[Int], o:Option[Int])=>{
    var result = 0
    //如果全局缓存存在则获取
    if(o.isDefined){
      result=o.get
    }
    for(i <- values){
      result+=i
    }
    Some(result)
  }).print(20)

  ssc.start()
  ssc.awaitTermination()
}
