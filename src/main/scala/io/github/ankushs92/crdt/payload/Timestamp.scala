package io.github.ankushs92.crdt.payload

trait Timestamp[T] {

  def getVal : T

  def compare(t1 : T, t2 : T)

}
