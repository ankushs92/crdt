package io.github.ankushs92.crdt.DeltaBased.buffers



import io.github.ankushs92.crdt.DeltaBased.WriteOp

import scala.collection.mutable

class SummingDeltaBuffer(replicaId : Int) extends DeltaBuffer[Int, SummingDeltaAdd, SummingDeltaGroup] {

  private var sendLocal = false
  private var posIncremented = false
  private var negIncremented = false
  private val BOTTOM_VALUE = 0.0
  private var localPosValue = BOTTOM_VALUE
  private var localNegValue = BOTTOM_VALUE
  private val recDeltaGroups = mutable.HashMap[Int, SummingDeltaGroup]() // Could have used a hashmap[int, int] instead, but  I feel going with a named DeltaGroup makes the code more readable

  override def getReplicaId: Int = replicaId

  override def bpAndccOptimization(nbrReplicaId: Int): Iterable[SummingDeltaGroup] = {
    val filtered = recDeltaGroups
      .filter { case (id, _) => nbrReplicaId != id } //BP
      .filter { case (_, deltaGroup) => // CC
        val alreadySentToReplica: Boolean = ccOptimizationMap.get(deltaGroup) match {
          case Some(replicaIds) => replicaIds.contains(nbrReplicaId)
          case None => false
        }
        !alreadySentToReplica
      }.values

    val local = Iterable(SummingDeltaGroup(replicaId, localPosValue, localNegValue, posPresent = posIncremented, negPresent = negIncremented))
      .filter { local =>
        val alreadySentToReplica = ccOptimizationMap.get(local) match {
          case Some(_) =>  true
          case None => false
        }
        !alreadySentToReplica && sendLocal
      }

    //in received delta groups, send the values for those replicas that have not been acknowledged
    local ++ filtered
  }

  override def add[W >: SummingDeltaAdd](id: Int, mode: WriteOp.Value, write: W): Unit =  {
    val recValue = write.asInstanceOf[SummingDeltaAdd].getValue
    mode match {
      case WriteOp.LOCAL =>
        write match {
          case PositiveValueAdd(_) =>
            localPosValue += recValue
            posIncremented = true
          case NegativeValueAdd(_) =>
            localNegValue += recValue
            negIncremented = true
        }
        sendLocal = true

      case WriteOp.REMOTE =>
        recDeltaGroups.get(id) match {
          case Some(oldDeltaGroup) =>
            write match {
              case PositiveValueAdd(_) =>
                val oldPos = oldDeltaGroup.pos
                recDeltaGroups.put(id, SummingDeltaGroup(id, Math.max(oldPos, recValue), oldDeltaGroup.neg, posPresent = true, oldDeltaGroup.negPresent))
              case NegativeValueAdd(_) =>
                val oldNeg = oldDeltaGroup.neg
                recDeltaGroups.put(id, SummingDeltaGroup(id, oldDeltaGroup.pos, Math.max(oldNeg, recValue), oldDeltaGroup.posPresent, negPresent = true))
            }

          case None =>
            write match {
              case PositiveValueAdd(_) =>
                recDeltaGroups.put(id, SummingDeltaGroup(id, recValue, BOTTOM_VALUE, posPresent = true, negPresent = false))
              case NegativeValueAdd(_) =>
                recDeltaGroups.put(id, SummingDeltaGroup(id, BOTTOM_VALUE, recValue, posPresent = false, negPresent = true))
            }
        }
    }
  }

  override def removeIfThresholdReached(deltaGroup: SummingDeltaGroup, threshold: Int): Unit = ccOptimizationMap.get(deltaGroup) match {
    case Some(replicaIds) =>
      if(replicaIds.size == threshold) {
        recDeltaGroups.foreach { case(id, oldDeltaGroup) =>
          if(oldDeltaGroup == deltaGroup) {
            recDeltaGroups.remove(id)
          }
        }
        sendLocal = false
        posIncremented = false
        negIncremented = false
        ccOptimizationMap.remove(deltaGroup)
      }
  }

  private def localDeltaGroup = "[" + localPosValue + "," + localNegValue + "]"
  override def toString: String = "Local Delta groups: " + localDeltaGroup + " Rec delta groups" + recDeltaGroups + " ccOptimizationMap " + ccOptimizationMap

}


