package com.scala.readwrite.example

import com.scala.readwrite.example.SystemType.SystemType
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}


object ReadWriteLauncher {

  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("ReadWriteApp").setMaster("local")

    println("Starting JOB!")

    val sc = new SparkContext(conf)

    def getTimerTaskByType(sc:SparkContext,st:SystemType,user:String,passw:String,readPath:String,writePath:String): ReadWriteTask={
      ReadWriteFactory.getReadWriteTask(sc,st,user,passw,readPath,writePath)
    }

    def timer(): Unit ={
      val timer = new java.util.Timer()

      //output directory should be
      // "hdfs://cluster/rimapolsky/test"

      val task1 = getTimerTaskByType(sc, SystemType.UNIX,"alex","12345","/Users/rimapolsky/myControlData.csv","/Users/rimapolsky/test");

      timer.schedule(task1,1000L, 10000L)
    }

    timer
  }

}
