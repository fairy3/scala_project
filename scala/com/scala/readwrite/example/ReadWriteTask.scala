package com.scala.readwrite.example

import java.io.{File, FileNotFoundException, IOException}

import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import com.scala.readwrite.example.StatusCode._
import org.apache.spark.rdd.RDD

abstract class ReadWriteTask(val sc:SparkContext, user:String, val passw:String, val path:String,val writePath:String) extends java.util.TimerTask with ReadWriteTrait{

  private var _max_retries_number: Integer = 2

  def getControlFile(fileName: String):ArrayBuffer[String]={
    val filesNamesList=ArrayBuffer[String]()
    try{
      for(line<- Source.fromFile(fileName).getLines){
        filesNamesList += line
        println(line)
      }
      filesNamesList

    } catch {
      case e: FileNotFoundException => println("Couldn't find file "+fileName)
        filesNamesList
      case e: IOException => println("Got an IOException" + e.getStackTrace)
        filesNamesList
    }
  }
  override def run()= {
    val filesNamesList: ArrayBuffer[String] = getControlFile(this.path);
    val numOfFiles: Int = filesNamesList.length

    if (filesNamesList.length == 0) {
      sendNotify(StatusCode.READ_FAILED);
    } else {

    println("control file contains list of " + numOfFiles + " files")
    var filesBuffer: ArrayBuffer[RDD[String]] = readDataFiles(filesNamesList, this.user, this.passw);

      if (filesBuffer.length < numOfFiles) {
        var retriesNumber = 0
        while (filesBuffer.length < numOfFiles && retriesNumber < _max_retries_number) {
          filesBuffer.clear()
          Thread.sleep(3000)
          filesBuffer = readDataFiles(filesNamesList, this.user, this.passw);
          retriesNumber += 1
        }
      }

      if (filesBuffer.length == numOfFiles) {
        val writesNumber: Integer = writeTo(filesBuffer, this.writePath)
        if (writesNumber == numOfFiles)
          sendNotify(StatusCode.SUCCESS)
        else
          sendNotify(StatusCode.WRITE_FAILED)
      }
      else
        sendNotify(StatusCode.READ_FAILED);


    }
  }

  def sendNotify(status:StatusCode)= {
    println(status.toString)
  }

}
