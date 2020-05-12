package io.github.ankushs92.crdt.DeltaBased

import java.net.InetSocketAddress

import io.github.ankushs92.crdt.DeltaBased.buffers.{GCounterDeltaBuffer, GCounterDeltaBufferAdd, GCounterDeltaGroup}
import io.github.ankushs92.crdt.payload.{GCounterPayload, GCounterState, IntVersionVector}
import io.github.ankushs92.crdt.{CRDTTypes, DeltaCRDTReplica, Neighbour}




case class GCounterDeltaCRDT(
                              id : Int,
                              syncInterval : Int,
                              addr : InetSocketAddress,
                              neighbours : List[Neighbour])
  extends DeltaCRDTReplica[Int, GCounterDeltaGroup ,GCounterPayload, GCounterDeltaBuffer, GCounterState, Int] {

  private val verVec = bottom.verVec
  private val deltaBuffer = new GCounterDeltaBuffer(id)

  override def getDeltaBuffer: GCounterDeltaBuffer = deltaBuffer

  override def getSyncInterval: Int = syncInterval

  override def merge(delta: GCounterPayload): Unit = synchronized {
    val nbrReplicaId = delta.nbrReplicaId
    val deltaGroups = delta.payload
    if(verVec.isEmpty) {
      deltaGroups.foreach { deltaGroup =>
        val idx = deltaGroup.replicaId - 1
        val curr = verVec.atIndex(idx)
        verVec.modifyAtIndex(idx, Math.max(deltaGroup.count, curr))
        deltaBuffer.add(nbrReplicaId, WriteOp.REMOTE, GCounterDeltaBufferAdd(deltaGroup.count))
      }
    }
    //Compute the difference between the current state and received payload, only add the diff to state and version vectors
    deltaGroups.foreach { deltaGroup =>
      val nbrReplicaId = deltaGroup.replicaId
      val idx = nbrReplicaId - 1
      val recReplicaCount = deltaGroup.count
      if(verVec.atIndex(idx) != recReplicaCount) {
        val curr = verVec.atIndex(idx)
        verVec.modifyAtIndex(idx, Math.max(recReplicaCount, curr))
        deltaBuffer.add(nbrReplicaId, WriteOp.REMOTE, GCounterDeltaBufferAdd(recReplicaCount))
      }
    }
  }

  override def getReplicaId: Int = id

  override def bottom: GCounterState = synchronized { GCounterState(IntVersionVector(totalReplicas)) }

  override def getName: String = CRDTTypes.G_COUNTER

  override def getAddr: InetSocketAddress = addr

  override def update(write: Int): Unit = synchronized {
    val idx = id - 1
    verVec.incrementBy(idx, write)
    deltaBuffer.add(id, WriteOp.LOCAL, GCounterDeltaBufferAdd(write))
  }

  override def query: Int = synchronized { verVec.viewOverState.sum }

  override def getCurrentState: GCounterState = synchronized { GCounterState(verVec) }

  override def getNeighbours: List[Neighbour] = neighbours
}
