package io.github.ankushs92.crdt.DeltaBased.buffers

import io.github.ankushs92.crdt.DeltaBased.WriteOp


import scala.collection.mutable

//BR Optimization, RR optimization and CC optimization
class GrowOnlySetDeltaBuffer[Value](replicaId : Int) extends DeltaBuffer[Int, GrowOnlySetDeltaBufferAdd[Value], Value] {

  private val localDeltaGroup = mutable.Set[Value]()
  private val recDeltaGroups = mutable.HashMap[Int, mutable.Set[Value]]()

  override def bpAndccOptimization(nbrReplicaId: Int): Iterable[Value] = {
    val filteredRec = recDeltaGroups.filter { case (id, _) => nbrReplicaId != id }
      .flatMap { case (_, state) =>
        state.filter { stateValue =>
          val alreadySentToReplica = ccOptimizationMap.get(stateValue) match {
            case Some(replicaIds) => replicaIds.contains(nbrReplicaId)
            case None => false
          }
          !alreadySentToReplica
        }
      }

    val filteredLocal = localDeltaGroup.filter { localVal =>
      val alreadySentToReplica = ccOptimizationMap.get(localVal) match {
        case Some(_) => true
        case None => false
      }
      !alreadySentToReplica
    }
    filteredLocal ++ filteredRec
  }

  override def add[W >: GrowOnlySetDeltaBufferAdd[Value]](id: Int, mode: WriteOp.Value, write: W): Unit = {
    val value = write.asInstanceOf[GrowOnlySetDeltaBufferAdd[Value]].value
    mode match {
      case WriteOp.LOCAL =>
        if (!localDeltaGroup.contains(value)) {
          localDeltaGroup += value
        }

      case WriteOp.REMOTE =>
        recDeltaGroups.get(id) match {
          case Some(deltaGroup) =>
            if (!deltaGroup.contains(value)) {
              deltaGroup += value
            }
          case None =>
            recDeltaGroups.put(id, mutable.HashSet[Value](value))
        }
    }
  }

  override def removeIfThresholdReached(value: Value, threshold: Int): Unit = {
    ccOptimizationMap.get(value) match {
      case Some(replicaIds) =>
        if(replicaIds.size == threshold) {
          recDeltaGroups.foreach { case(_, values) =>
            if(values.contains(value)) {
              values.remove(value)
            }
          }
          if(localDeltaGroup.contains(value)) {
            localDeltaGroup.remove(value)
          }
          ccOptimizationMap.remove(value)
        }
    }
  }


  override def toString: String = "Local Delta groups: " + localDeltaGroup + " Rec delta groups" + recDeltaGroups + " ccOptimizationMap " + ccOptimizationMap

  override def getReplicaId: Int = replicaId

}
