package io.github.ankushs92.crdt.DeltaBased.buffers

abstract class DeltaBufferAdd

trait SummingDeltaAdd extends DeltaBufferAdd {
  def getValue : Double
}


case class GCounterDeltaBufferAdd(value : Int) extends DeltaBufferAdd

case class GrowOnlySetDeltaBufferAdd[Value](value : Value) extends DeltaBufferAdd

case class PositiveValueAdd(value : Double) extends SummingDeltaAdd {
  override def getValue: Double = value
}

case class NegativeValueAdd(value : Double) extends SummingDeltaAdd {
  override def getValue: Double = value
}