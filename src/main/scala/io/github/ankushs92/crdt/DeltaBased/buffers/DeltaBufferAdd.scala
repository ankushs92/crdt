package io.github.ankushs92.crdt.DeltaBased.buffers

trait DeltaBufferAdd {
}

trait SummingDeltaAdd extends DeltaBufferAdd {
  def getValue : Double
}


case class GCounterDeltaBufferAdd(value : Int) extends DeltaBufferAdd {
}

case class GrowOnlySetDeltaBufferAdd[Value](value : Value) extends DeltaBufferAdd{
}

case class PositiveValueAdd(id : Int, value : Double) extends SummingDeltaAdd {
  override def getValue: Double = value
}

case class NegativeValueAdd(id : Int, value : Double) extends SummingDeltaAdd {
  override def getValue: Double = value
}
