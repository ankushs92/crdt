package io.github.ankushs92.crdt

import java.net.InetSocketAddress

import akka.actor.ActorRef
import com.typesafe.scalalogging.Logger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

case class OneWayConnectedReplica[T, U <: Payload[U], V](
                                                 replica : CRDTReplica[T, U, V],
                                                 otherReplica : InetSocketAddress,
                                                 tcpClient : ActorRef
                                                  )
{

  //Two way exchange
  def exchangeState : Future[Unit]  = Future {
    val replicaState = replica.getCurrentState.getBytes


  }

}

trait CRDTReplica[StateType, State <: Payload[State], QueryResult]  {

  val logger = Logger(this.getClass)

  def getName : String

  def getAddr : InetSocketAddress

  def update(write : StateType)

  def query : QueryResult

  def getCurrentState : State

  def connectTo(replica : InetSocketAddress) : OneWayConnectedReplica[StateType, State, QueryResult] = {
    val globalSystem = AkkaActorLauncher.globalSystem
    null
//    val crdtTcpClient = globalSystem.actorOf(Props(classOf[GCounterStateCRDTReplica], replica, null))
//    new OneWayConnectedReplica[StateType, State, QueryResult](this, replica, crdtTcpClient)
  }


  override def toString: String = getName + ";" + getAddr

}

trait StateCRDTReplica[StateType, State <: Payload[State], QueryResult] extends CRDTReplica[StateType, State, QueryResult]{

  def merge(payload1: State, payload2 : State) : State

  override def toString: String = getCurrentState.toString

}


object t extends App {
  val replica = SummingStateCRDT(1, 5, null)
  val otherReplica = SummingStateCRDT(2, 5, null)


  val connectedCrdts = replica.connectTo(otherReplica.addr)
  connectedCrdts.exchangeState



}