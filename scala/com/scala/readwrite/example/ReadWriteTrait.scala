package com.scala.readwrite.example

import java.io.File

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

trait ReadWriteTrait {
  def readDataFiles(filesList:ArrayBuffer[String],user:String,passw:String):ArrayBuffer[RDD[String]]
  def getControlFile(fileName: String):ArrayBuffer[String]
  def writeTo(filesList:ArrayBuffer[RDD[String]],writePath:String):Integer

}
