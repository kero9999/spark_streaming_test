package com.kero99.wp

import java.util.Properties

import com.kero99.wp.Test.getClass
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
/**
  *
  * @author wp
  * @date 2019-09-04 15:25
  *
  */
object Test extends App {

  var words = List("hadoop","HDFS","mapreduce","azkaban","sqoop","zookeeper","yarn","hive","hbase","phoenix","redis","spark")
  val random = new java.util.Random
  val timer = new java.util.Timer()
  timer.schedule(new java.util.TimerTask {
    var index=1;
    override def run(): Unit = {
      if(index<201){
        val id = index.toString
        index+=1
        val msg = words.mkString(" ")
        WpProducer.sendmessage("wp",id,msg)
      }


//      val line = new java.lang.StringBuilder
//      for(i <- 0 until random.nextInt(100)){
//        val index = random.nextInt(words.length)
//        line.append(words(index)).append(" ")
//      }
//      var id=(index+=1).toString;
//      println(s"生产:id:${id},value:${line}")
//      WpProducer.sendmessage("wp",id,line.toString)
    }
  },0,1000)
}
object WpProducer {
  def sendmessage(topic:String,key:String,msg:String): Unit = {
    val props = new Properties()
    props.load(getClass.getClassLoader.getResourceAsStream("producer.properties"))
    val producer = new KafkaProducer[String, String](props)
    val message = new ProducerRecord[String,String](topic,key,msg)
    producer.send(message)  //发送到指定的topic
  }
  def sendmessage(topic:String,msg:String): Unit = {
    val props = new Properties()
    props.load(getClass.getClassLoader.getResourceAsStream("producer.properties"))
    val producer = new KafkaProducer[String, String](props)
    val message = new ProducerRecord[String,String](topic,msg)
    producer.send(message)  //发送到指定的topic
  }
}