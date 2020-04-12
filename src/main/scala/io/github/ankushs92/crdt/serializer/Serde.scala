package io.github.ankushs92.crdt.serializer

trait Serde[IN, OUT] {

  def serialize(in: IN): OUT

  def deserialize(out: OUT): IN

}
