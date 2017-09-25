package com.dematic.labs.dsp.data

import java.nio.charset.Charset
import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

object Utils {
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def toJson(value: Map[Symbol, Any]): String = {
    toJson(value map { case (k, v) => k.name -> v })
  }

  def toJson(value: Any): String = {
    mapper.writeValueAsString(value)
  }

  def toMap[V](json: String)(implicit m: Manifest[V]): Map[String, V] = fromJson[Map[String, V]](json)

  def fromJson[T](json: String)(implicit m: Manifest[T]): T = {
    mapper.readValue[T](json)
  }

  def toBytes(json: String): Array[Byte] = {
    json.getBytes(Charset.defaultCharset())
  }

  // todo: look int spark sql timestamp conversions
  def toTimeStamp(timeStr: String): Timestamp = {
    val dateFormat1: DateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateFormat2: DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

    val date: Option[Timestamp] = {
      try {
        Some(new Timestamp(dateFormat1.parse(timeStr).getTime))
      } catch {
        case e: java.text.ParseException =>
          Some(new Timestamp(dateFormat2.parse(timeStr).getTime))
      }
    }
    date.getOrElse(Timestamp.valueOf(timeStr))
  }
}