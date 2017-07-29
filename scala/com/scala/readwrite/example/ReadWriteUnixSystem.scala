package com.scala.readwrite.example


import org.apache.spark.{SparkContext, SparkFiles}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

class ReadWriteUnixSystem(sc:SparkContext,user:String,passw:String,path:String,writePath:String) extends ReadWriteTask(sc:SparkContext,user:String,passw:String,path:String,writePath:String){



  override def readDataFiles(filesNames:ArrayBuffer[String],user:String,passw:String):ArrayBuffer[RDD[String]]={

    println("read data files from UNIX FS")
    var count:Integer=0
    val filesList:ArrayBuffer[RDD[String]]=new ArrayBuffer[RDD[String]]()
    for (name <- filesNames) {
      var path:String=""
      path += name
      val dataSource = path
      sc.addFile(dataSource)
      var fileName = SparkFiles.get(dataSource.split("/").last)
      var datafile = sc.textFile(fileName)
      filesList += datafile;
    }
    filesList

  }

  def writeTo(files:ArrayBuffer[RDD[String]],writePath:String): Integer ={
    println("write UNIX files to " + writePath)
    var counter:Integer=0
    var path:String=writePath
    for (rdd <- files) {
      rdd.saveAsTextFile(writePath+counter)
      counter += 1
    }

    counter
  }



}
