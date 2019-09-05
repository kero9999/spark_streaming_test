package com.kero99.wp

import redis.clients.jedis.JedisPoolConfig
import redis.clients.jedis.JedisPool
import redis.clients.jedis.Jedis
/**
  *
  * @author wp
  * @date 2019-09-05 14:44
  *
  */
object JedisConn {
  private val cfg = new JedisPoolConfig
  cfg.setMaxTotal(50)
  cfg.setMaxIdle(10)
  private val pool = new JedisPool(cfg,"192.168.52.42",6379)


  def open()={
    pool.getResource
  }

  def close(jedis:Jedis): Unit ={
    jedis.close()
  }


  sys.addShutdownHook(new Thread(){
    pool.close()
  })

}
