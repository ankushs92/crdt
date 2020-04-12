package io.github.ankushs92.crdt.CvRDT.payload

import java.nio.ByteBuffer

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import io.github.ankushs92.crdt.serializer.JsonSerde

trait Payload[T] {
  def getBytes : ByteBuffer
  def from(bytes : ByteBuffer) : T
}


@JsonIgnoreProperties(ignoreUnknown = true)
case class GCounterPayload(verVec : IntVersionVector) extends Payload[GCounterPayload] {
  override def getBytes : ByteBuffer = ByteBuffer.wrap(new JsonSerde[GCounterPayload].serialize(this).getBytes)

  override def from(bytes: ByteBuffer): GCounterPayload = ???
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class PNCounterPayload(incVerVec : IntVersionVector, decVerVec : IntVersionVector) extends Payload[PNCounterPayload] {
  override def getBytes : ByteBuffer = ByteBuffer.wrap(new JsonSerde[PNCounterPayload].serialize(this).getBytes)

  override def from(bytes: ByteBuffer): PNCounterPayload = ???
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class SummingPayload(positiveVerVec : DoubleVersionVector, negVerVec : DoubleVersionVector) extends Payload[SummingPayload] {
  override def getBytes: ByteBuffer = ByteBuffer.wrap(new JsonSerde[SummingPayload].serialize(this).getBytes)

  override def from(bytes: ByteBuffer): SummingPayload = ???
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class AveragingPayload(positiveVerVec : DoubleVersionVector, negVerVec : DoubleVersionVector, countVec : IntVersionVector) extends Payload[AveragingPayload] {
  override def getBytes : ByteBuffer = ByteBuffer.wrap(new JsonSerde[AveragingPayload].serialize(this).getBytes)

  override def from(bytes: ByteBuffer): AveragingPayload = ???
}

object t extends App {
  val array = Array(1, 2, 3)
  val array2 = Array(4,5,6)

  val array3 = array ++ array2
//  ByteString.fromByteBuffer()

}



