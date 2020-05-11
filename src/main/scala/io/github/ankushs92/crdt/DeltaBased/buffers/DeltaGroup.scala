package io.github.ankushs92.crdt.DeltaBased.buffers

trait DeltaGroup

case class GCounterDeltaGroup(replicaId : Int, count : Int) extends DeltaGroup
case class SummingDeltaGroup(replicaId : Int, pos : Double, neg : Double, posPresent : Boolean, negPresent : Boolean) extends DeltaGroup
