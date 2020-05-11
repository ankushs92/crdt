package io.github.ankushs92.crdt.DeltaBased.buffers

import io.github.ankushs92.crdt.DeltaBased.WriteOp


import scala.collection.mutable


trait DeltaBuffer[ID, WriteValue, QueryValue] {

  val ccOptimizationMap: mutable.Map[QueryValue, mutable.Set[ID]] = mutable.HashMap[QueryValue, mutable.Set[ID]]()

  def getReplicaId : Int

  /**
    * CC optimization : This would be called by one of the methods to remove a value from the delta buffer that no longer
    * needs to be propogated to any replica
    * BP optimization : Out of all the values in the delta buffer, this optimization returns those values that were not received by all replicas
    * except for the replicaId passed
    * Together, they mean : Return all values that were not propagated by this replica in the first place, as well as omit
    * those values in local or received buffers that have already been sent to this replica
    */
  def bpAndccOptimization(nbrReplicaId : Int) : Iterable[QueryValue]

  def add(id : ID, mode : WriteOp.Value, write : WriteValue) : Unit

  def addAll(id : ID, mode : WriteOp.Value, writes : Iterable[WriteValue]) : Unit = writes.foreach { add(id, mode, _) }

  /**
    * Acknowledge that the value has reached the replica with passed id
    */
  def acknowledge(id : ID, write : QueryValue) = ccOptimizationMap.get(write) match {
    case Some(replicaIds) =>
      replicaIds += id
    case None =>
      ccOptimizationMap.put(write, mutable.HashSet[ID](id))
  }

  def removeIfThresholdReached(value : QueryValue, threshold : Int)

}