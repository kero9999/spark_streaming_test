package com.kero99.wp

import java.util
import java.util.Properties

import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.reflect.ClassTag

/**
  *
  * @author wp
  * @date 2019-09-04 16:10
  *
  */
object Test extends App {
  val cfg = new SparkConf().setMaster("local[2]").setAppName("wc")
  val ssc = new StreamingContext(cfg,Seconds(5))


  val kafkaParams=Map("bootstrap.servers"->"apache01:9092,apache02:9092,apache03:9092")

  val topics = Set("wp")
  val dStream=KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)

  dStream.foreachRDD(rdd=>{
  })


  ssc.start()
  ssc.awaitTermination()
}


