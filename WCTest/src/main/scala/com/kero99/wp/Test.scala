package com.kero99.wp

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  * @author wp
  * @date 2019-09-04 14:17
  *
  */
object Test extends App{
  val cfg = new SparkConf().setAppName("wc").setMaster("local[2]")
  val ssc = new StreamingContext(cfg,Seconds(1))


  val dStream=ssc.socketTextStream("apache01",11111)
  dStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

  ssc.start()
  ssc.awaitTermination()

}
