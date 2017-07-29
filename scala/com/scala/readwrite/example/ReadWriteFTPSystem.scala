package com.scala.readwrite.example

import java.io.File

import org.apache.spark.{SparkContext, SparkFiles}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

class ReadWriteFTPSystem(sc:SparkContext,user:String, passw:String, readPath:String,writePath:String) extends ReadWriteTask(sc:SparkContext,user:String,passw:String, readPath:String,writePath:String) {
  override def readDataFiles(filesNames:ArrayBuffer[String],user:String,passw:String):ArrayBuffer[RDD[String]]={
    println("read Data Files from ftp system")

    val filesList:ArrayBuffer[RDD[String]]=new ArrayBuffer[RDD[String]]()
    //<user>:<password>@<host>:<port>/<url-path>
    for (name <- filesNames) {
      var path:String=readPath
      path += "/"
      path += name
      val dataSource = "ftp://user:"+this.passw+this.user+path
      sc.addFile(dataSource)
      var fileName = SparkFiles.get(dataSource.split("/").last)
      var file = sc.textFile(fileName)
      filesList += file;
    }

    filesList
  }

  def writeTo(files:ArrayBuffer[RDD[String]],writePath:String): Integer = {
    println("write ftp files to " + writePath)

    var counter:Integer=0
    var path:String=writePath
    for (rdd <- files) {
      rdd.saveAsTextFile(path)
      counter += 1
    }

    counter
  }
}
