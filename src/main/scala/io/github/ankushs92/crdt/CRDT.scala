package io.github.ankushs92.crdt

import java.net.InetSocketAddress

import akka.actor.ActorRef
import com.typesafe.scalalogging.Logger
import io.github.ankushs92.crdt.payload.Payload

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

case class OneWayConnectedReplica[T, U <: Payload[U], V](
                                                 replica : CRDTReplica[T, U, V],
                                                 otherReplica : InetSocketAddress,
                                                 tcpClient : ActorRef
                                                  )
{

  //Two way exchange
  def exchangeState(implicit m : Manifest[U]) : Future[Unit]  = Future {
    val replicaState = replica.getCurrentState.getBytes


  }

}

trait CRDTReplica[StateType, State <: Payload[State], QueryResult]  {

  val logger = Logger(this.getClass)

  def initialValue : State
  def getName : String

  def getAddr : InetSocketAddress

  def update(write : StateType)

  def query : QueryResult

  def getCurrentState : State

  override def toString: String = getName + ";" + getAddr

}

trait StateCRDTReplica[StateType, State <: Payload[State], QueryResult] extends CRDTReplica[StateType, State, QueryResult]{

  def merge(replicaPayload : State) : State

  override def toString: String = getCurrentState.toString

}

