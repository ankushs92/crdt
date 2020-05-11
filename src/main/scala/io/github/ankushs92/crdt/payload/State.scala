package io.github.ankushs92.crdt.payload

import java.nio.ByteBuffer

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import io.github.ankushs92.crdt.DeltaBased.buffers.{GCounterDeltaGroup, SummingDeltaGroup}
import io.github.ankushs92.crdt.serializer.JsonSerde
import io.github.ankushs92.crdt.util.Json

import scala.collection.mutable


trait Metadata {
  def getNbrReplicaId : Int
  def getCrdtType : String
}

trait CRDTState[T] {

}

trait Payload[T] extends Metadata {
  def getBytes (implicit m : Manifest[T]): ByteBuffer
  def from(bytes : ByteBuffer) : T
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class SummingPayload(nbrReplicaId : Int, crdtType : String, payload : Iterable[SummingDeltaGroup]) extends Payload[SummingPayload]  {
  override def getBytes(implicit m: Manifest[SummingPayload]): ByteBuffer = ByteBuffer.wrap(new JsonSerde[SummingPayload].serialize(this).getBytes)

  override def from(bytes: ByteBuffer): SummingPayload = Json.toObject(bytes.toString)

  override def getNbrReplicaId: Int = nbrReplicaId

  override def getCrdtType: String = crdtType
}


case class GrowOnlySetPayload[T](nbrReplicaId : Int, crdtType : String, payload : Set[T]) extends Payload[GrowOnlySetPayload[T]]  {
  override def getBytes(implicit m: Manifest[GrowOnlySetPayload[T]]): ByteBuffer = ByteBuffer.wrap(new JsonSerde[GrowOnlySetPayload[T]].serialize(this).getBytes)

  override def from(bytes: ByteBuffer): GrowOnlySetPayload[T] = Json.toObject(bytes.toString)

  override def getNbrReplicaId: Int = nbrReplicaId

  override def getCrdtType: String = crdtType
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class GCounterPayload(nbrReplicaId : Int, crdtType : String, payload : Iterable[GCounterDeltaGroup]) extends Payload[GCounterPayload] {

  override def getBytes(implicit m : Manifest[GCounterPayload]) : ByteBuffer = ByteBuffer.wrap(new JsonSerde[GCounterPayload].serialize(this).getBytes)

  override def from(bytes: ByteBuffer): GCounterPayload = Json.toObject(bytes.toString)

  override def getNbrReplicaId: Int = nbrReplicaId

  override def getCrdtType: String = crdtType
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class GrowOnlySetState[T](state : mutable.Set[T]) extends CRDTState[GCounterState] {
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class GCounterState(verVec : IntVersionVector) extends CRDTState[GCounterState] {

}


@JsonIgnoreProperties(ignoreUnknown = true)
case class SummingState(positiveVerVec : DoubleVersionVector, negVerVec : DoubleVersionVector) extends CRDTState[SummingState] {
}


@JsonIgnoreProperties(ignoreUnknown = true)
case class PNCounterPayload(incVerVec : IntVersionVector, decVerVec : IntVersionVector) extends CRDTState[PNCounterPayload] {

}



@JsonIgnoreProperties(ignoreUnknown = true)
case class AveragingPayload(positiveVerVec : DoubleVersionVector, negVerVec : DoubleVersionVector, countVec : IntVersionVector) extends CRDTState[AveragingPayload] {
}

////@JsonIgnoreProperties(ignoreUnknown = true)
////case class LWWRegisterPayload[T, Timestamp <: Order[Timestamp]](timestamp : LamportTimestamp, data : T)
////    extends CRDTState[LWWRegisterPayload[T, Timestamp]] {
//
//
//}
//
