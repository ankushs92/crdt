package io.github.ankushs92.crdt.StateBased

import java.net.InetSocketAddress

import io.github.ankushs92.crdt.StateCRDTReplica
import io.github.ankushs92.crdt.payload.{LWWRegisterPayload, LamportTimestamp}

case class LWWRegisterStateCRDTReplica[T](
                                           replicaId : Int,
                                           replicaCount : Int,
                                           initial : T,
                                           addr : InetSocketAddress) extends StateCRDTReplica[T, LWWRegisterPayload[T], T] {

  private val timestamp = initialValue.timestamp
  private var value = initialValue.data

  override def merge(replicaPayload: LWWRegisterPayload[T]): LWWRegisterPayload[T] = this.synchronized {
    val replicaTimestamp = replicaPayload.timestamp
    if(timestamp.lessThan(replicaTimestamp)) {
      timestamp.setNew(replicaTimestamp.state)
      value = replicaPayload.data
      new LWWRegisterPayload[T](replicaTimestamp, replicaPayload.data)
    }
    else {
      timestamp.setNew(timestamp.state)
      new LWWRegisterPayload[T](timestamp, value)
    }
  }

  override def getName: String = "lww-register"

  override def getAddr: InetSocketAddress = addr

  override def update(newVal: T): Unit = this.synchronized {
    timestamp.increment
    value = newVal
  }

  override def query: T = value

  override def getCurrentState: LWWRegisterPayload[T] = new LWWRegisterPayload[T](timestamp, value)

  override def initialValue: LWWRegisterPayload[T] = this.synchronized { new LWWRegisterPayload(new LamportTimestamp, initial) }
}


object T extends App {
  val crdt1 = LWWRegisterStateCRDTReplica[String](1, 5, "", null)
  val crdt2 = LWWRegisterStateCRDTReplica[String](1, 5, "", null)

  crdt1.update("old")

  println(crdt1.getCurrentState)
  crdt1.update("new")
  crdt1.update("newNew")

  crdt2.update("replica2new")
  println(crdt2.getCurrentState)


  crdt1.merge(crdt2.getCurrentState)
  println(crdt1.query)
  println(crdt2.query)

}