package io.github.ankushs92.crdt.CvRDT.payload

import scala.collection.mutable.ArrayBuffer

case class IntVersionVector(size : Int) extends VersionVector[Int] {

  private val array = ArrayBuffer.fill(size)(0)

  override def getSize: Int = size

  override def currentState: ArrayBuffer[Int] = array

  override def incrementBy(idx : Int, value : Int) = {
    require(idx <= size, "index cannot be greater than size of Version vector")
    currentState(idx) += value
  }

  override def atIndex(idx : Int) = {
    require(idx <= size, "index cannot be greater than size of Version vector")
    currentState(idx)
  }

  override def modifyAtIndex(idx : Int, value : Int) = {
    require(idx <= size, "index cannot be greater than size of Version vector")
    currentState(idx) = value
  }

  override def viewOverState: Array[Int] = currentState.toArray

}

