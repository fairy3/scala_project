package com.scala.readwrite.example

import com.scala.readwrite.example.SystemType._
import org.apache.spark.SparkContext

object ReadWriteFactory {
  def getReadWriteTask(sc:SparkContext,sType:SystemType,user:String,passw:String,readPath:String, writePath:String): ReadWriteTask= {
    sType match {
      case UNIX => new ReadWriteUnixSystem(sc,user, passw, readPath, writePath)
      case FTP => new ReadWriteFTPSystem(sc,user, passw, readPath, writePath)
      case _ => null
    }
  }
}
