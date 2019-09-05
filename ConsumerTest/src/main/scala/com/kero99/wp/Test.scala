package com.kero99.wp

import java.text.SimpleDateFormat
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
  ssc.sparkContext.setLogLevel("WARN")


  val kafkaParams=Map("bootstrap.servers"->"apache01:9092,apache02:9092,apache03:9092")

  val topics = Set("wp")
  val dStream=KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)

  dStream.flatMap(_._2.split(" ")).filter(!_.isEmpty).map((_,1)).reduceByKey(_+_).foreachRDD(rdd=>{
    rdd.foreachPartition(f=>{
      val fd = new SimpleDateFormat("yyyyMMdd_HH_mm")
      val hkey = fd.format(new java.util.Date())
      val jedis = JedisConn.open()
      jedis.select(6)
      f.foreach(x=>{
          val hfield = x._1
          if(jedis.hexists(hkey,hfield)){
            //已存在 累加
            jedis.hincrBy(hkey,hfield,x._2)
            println(s"累加:key:${hkey},field:${hfield},value:${x._2}")
          }else{
            //不存在 创建
            jedis.hset(hkey,hfield,x._2.toString)
            println(s"创建:key:${hkey},field:${hfield},value:${x._2}")
          }
      })
      JedisConn.close(jedis)
    })
  })


  ssc.start()
  ssc.awaitTermination()
}


