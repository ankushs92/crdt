package io.github.ankushs92.crdt.DeltaBased.buffers


import io.github.ankushs92.crdt.DeltaBased.WriteOp

import scala.collection.mutable

import scala.collection.mutable

class GCounterDeltaBuffer(replicaId : Int) extends DeltaBuffer[Int, GCounterDeltaBufferAdd, GCounterDeltaGroup] {

  private val INITIAL_COUNT = 0
  private var sendLocal = true
  private var localDeltaGroup : Int = INITIAL_COUNT
  private val recDeltaGroups = mutable.HashMap[Int, GCounterDeltaGroup]() // Could have used a hashmap[int, int] instead, but  I feel going with a named DeltaGroup makes the code more readable

  override def bpAndccOptimization(nbrReplicaId: Int): Iterable[GCounterDeltaGroup] = {
    val filtered = recDeltaGroups
      .filter { case (id, _) => nbrReplicaId != id } //BP
      .filter { case (_, deltaGroup) => // CC
        val alreadySentToReplica: Boolean = ccOptimizationMap.get(deltaGroup) match {
          case Some(replicaIds) => replicaIds.contains(nbrReplicaId)
          case None => false
        }
        !alreadySentToReplica
      }.values

    val local = Iterable(GCounterDeltaGroup(replicaId, localDeltaGroup))
      .filter { local =>
        val alreadySentToReplica = ccOptimizationMap.get(local) match {
          case Some(_) =>  true
          case None => false
        }
        !alreadySentToReplica && local.count != INITIAL_COUNT && sendLocal
      }

    //in received delta groups, send the values for those replicas that have not been acknowledged
    local ++ filtered
  }

  override def add[W >: GCounterDeltaBufferAdd](id: Int, mode: WriteOp.Value, write: W): Unit = {
    val value = write.asInstanceOf[GCounterDeltaBufferAdd].value
    mode match  {
      case WriteOp.LOCAL =>
        localDeltaGroup += value
        sendLocal = true

      case WriteOp.REMOTE =>
        recDeltaGroups.get(id) match {
          case Some(oldDeltaGroup) =>
            recDeltaGroups.put(id, GCounterDeltaGroup(id, Math.max(oldDeltaGroup.count, value)))
          case None =>
            recDeltaGroups.put(id, GCounterDeltaGroup(id, value))
        }
    }
  }

  override def removeIfThresholdReached(deltaGroup: GCounterDeltaGroup, threshold: Int): Unit = ccOptimizationMap.get(deltaGroup) match {
    case Some(replicaIds) =>
      if(replicaIds.size == threshold) {
        recDeltaGroups.foreach { case(id, oldDeltaGroup) =>
          if(oldDeltaGroup == deltaGroup) {
            recDeltaGroups.remove(id)
          }
        }
        sendLocal = false
        ccOptimizationMap.remove(deltaGroup)
      }
  }

  override def getReplicaId: Int = replicaId

  override def toString: String = "Local Delta groups: " + localDeltaGroup + " Rec delta groups" + recDeltaGroups + " ccOptimizationMap " + ccOptimizationMap

}
