package io.github.ankushs92.crdt.payload

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

trait VersionVector[T]  {

  def isEmpty : Boolean

  def getSize : Int

  def currentState : ArrayBuffer[T] //Initialized to an empty ArrayBuffer

  //Read only view
  def viewOverState : Array[T]

  def incrementBy(idx : Int, value : T)

  def atIndex(idx : Int) : T

  def modifyAtIndex(idx : Int, value : T)

  def greaterThan(otherVec : VersionVector[T]) = {
    val thisState = currentState
    val otherState = otherVec.currentState
    var bool = true
    breakable {
      // I don't like this, needs to change.
      for((value, idx) <- thisState.iterator.zipWithIndex) {
        var thisVecVal = value
        var otherVecVal = otherState(idx)
        if(otherVec.isInstanceOf[IntVersionVector]) {
          if(thisVecVal.asInstanceOf[Int] < otherVecVal.asInstanceOf[Int]) {
            bool = false
            break
          }
        }
        else {
          if(thisVecVal.asInstanceOf[Double] < otherVecVal.asInstanceOf[Double]) {
            bool = false
            break
          }
        }
      }
    }
    bool
  }

  override def toString: String = "[" + viewOverState.mkString(",") + "]"

}

