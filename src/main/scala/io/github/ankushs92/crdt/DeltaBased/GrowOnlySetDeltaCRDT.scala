package io.github.ankushs92.crdt.DeltaBased

import java.net.InetSocketAddress

import io.github.ankushs92.crdt.Neighbour
import io.github.ankushs92.crdt.DeltaBased.buffers.{GCounterDeltaBuffer, GCounterDeltaBufferAdd, GCounterDeltaGroup, GrowOnlySetDeltaBuffer, GrowOnlySetDeltaBufferAdd}
import io.github.ankushs92.crdt.payload.{GCounterPayload, GCounterState, GrowOnlySetPayload, GrowOnlySetState, IntVersionVector}
import io.github.ankushs92.crdt.{CRDTTypes, DeltaCRDTReplica, Neighbour}

import scala.collection.mutable


case class GrowOnlySetDeltaCRDT[T](
                                    replicaId : Int,
                                    syncInterval : Int,
                                    addr : InetSocketAddress,
                                    neighbours : List[Neighbour])
                                    extends DeltaCRDTReplica[T, T, GrowOnlySetPayload[T], GrowOnlySetDeltaBuffer[T], GrowOnlySetState[T], Set[T]]
{

  private val deltaBuffer = new GrowOnlySetDeltaBuffer[T](replicaId)
  private val state = bottom.state

  override def getSyncInterval(): Int = syncInterval

  override def merge(delta: GrowOnlySetPayload[T]): Unit = synchronized {
    val nbrReplicaId = delta.nbrReplicaId
    val payload = delta.payload
    val diff = payload -- state
    if(state.isEmpty) {
      payload.foreach { state += _ }
      deltaBuffer.addAll(nbrReplicaId, WriteOp.REMOTE, payload.map { GrowOnlySetDeltaBufferAdd(_) } )
    }
    //Only add diff to the delta buffer if there is indeed a difference between states
    if(diff.nonEmpty) {
      diff.foreach { state += _ }
      deltaBuffer.addAll(nbrReplicaId, WriteOp.REMOTE, diff.map { GrowOnlySetDeltaBufferAdd(_) })
    }
  }

  override def bottom: GrowOnlySetState[T] = synchronized { GrowOnlySetState(mutable.Set[T]()) }

  override def getName: String = CRDTTypes.G_SET

  override def getAddr: InetSocketAddress = addr

  override def update(write: T): Unit = synchronized {
    //add it to local state
    //If the state already contains the value, dont add it to the delta buffers. this way a particular value will eventually stop propogating to replicas
    if(!state.contains(write) || state.isEmpty) {
      state += write
      deltaBuffer.add(replicaId, WriteOp.LOCAL, GrowOnlySetDeltaBufferAdd(write))
    }
  }

  override def query: Set[T] = synchronized { state.toSet } // Making it immutable

  //TODO : Change this to immutable read only state
  override def getCurrentState: GrowOnlySetState[T] = synchronized { new GrowOnlySetState[T](state) }

  override def getNeighbours: List[Neighbour] = neighbours

  override def getDeltaBuffer: GrowOnlySetDeltaBuffer[T] = synchronized { deltaBuffer }

  override def getReplicaId: Int = replicaId

}
