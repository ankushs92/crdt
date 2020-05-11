package io.github.ankushs92.crdt.payload

import scala.collection.mutable.ArrayBuffer

case class DoubleVersionVector(size : Int) extends VersionVector[Double] {

  private val array = ArrayBuffer.fill(size)(0.0)

  override def getSize: Int =  size

  override def currentState : ArrayBuffer[Double] = array

  override def incrementBy(idx: Int, value : Double): Unit = {
    require(idx <= size, "index cannot be greater than size of Version vector")
    currentState(idx) += value
  }

  override def atIndex(idx: Int): Double = {
    require(idx <= size, "index cannot be greater than size of Version vector")
    currentState(idx)
  }

  override def modifyAtIndex(idx: Int, value: Double): Unit = {
    require(idx <= size, "index cannot be greater than size of Version vector")
    currentState(idx) = value
  }

  override def viewOverState: Array[Double] = currentState.toArray

  override def isEmpty: Boolean = array.isEmpty
}
