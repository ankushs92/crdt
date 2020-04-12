package io.ankushs92.util

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}

object Json {

  private val mapper = new ObjectMapper with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  //Holy shit, Batman. This manifest was introduced to the code after a google search, apparently
  //mixing java with scala is not as straightforward as is advertized
  def toObject[T : Manifest](json: String) = mapper.readValue[T](json)

  def encode(obj: Any): String = {
    try
      mapper.writer.writeValueAsString(obj)
    catch {
      case ex: Exception =>
        Strings.EMPTY
    }
  }

}
