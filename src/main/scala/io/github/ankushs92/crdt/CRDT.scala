package io.github.ankushs92.crdt

import java.net.InetSocketAddress

import com.typesafe.scalalogging.Logger
import io.github.ankushs92.crdt.DeltaBased.buffers.{DeltaBuffer, DeltaBufferAdd}
import io.github.ankushs92.crdt.payload.Payload


trait CRDTReplica[StateType, State, QueryResult]  {

  val logger = Logger(this.getClass)

  def getReplicaId : Int
  def bottom : State
  def getName : String

  def getAddr : InetSocketAddress

  def update(write : StateType)

  def query : QueryResult

  //This is just meant to debug, should not be used in production environments as it returns mutable state
  def getCurrentState : State
  def getNeighbours : List[Neighbour]

  override def toString: String = getName + ";" + getAddr
  val totalReplicas: Int = getNeighbours.size + 1
  val totalNbrs : Int = getNeighbours.size

}

trait StateCRDTReplica[StateType, State <: Payload[State], QueryResult] extends CRDTReplica[StateType, State, QueryResult]{

  def merge(replicaPayload : State) : State

  override def toString: String = getCurrentState.toString

}

trait DeltaCRDTReplica[
    StateType,
    DeltaBufferQueryType,
    DeltaPayload <: Payload[DeltaPayload],
    DeltaBufferType <: DeltaBuffer[Int, DeltaBufferAdd, DeltaBufferQueryType],
    State,
    QueryResult] extends CRDTReplica[StateType, State, QueryResult] {

  //Onlt meant for debugging
  def getDeltaBuffer : DeltaBufferType

  def replicaAck(ackReplicaId : Int, recDeltaBuffers : Iterable[DeltaBufferQueryType]) = recDeltaBuffers.foreach { value =>
    getDeltaBuffer.acknowledge(ackReplicaId, value)
    getDeltaBuffer.removeIfThresholdReached(value, totalNbrs)
  }

  //Sync Interval is in MS. After every _ ms, sync will take place between
  def getSyncInterval : Int

  def merge(delta : DeltaPayload) : Unit

  override def toString: String = getCurrentState.toString
}
