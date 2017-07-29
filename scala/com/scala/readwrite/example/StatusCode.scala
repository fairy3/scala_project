package com.scala.readwrite.example

object StatusCode extends Enumeration{
  type StatusCode = Value
  val SUCCESS, READ_FAILED, WRITE_FAILED= Value
}
