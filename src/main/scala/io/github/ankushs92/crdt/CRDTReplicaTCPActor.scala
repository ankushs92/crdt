package io.github.ankushs92.crdt

import akka.actor.{Actor, ActorSystem}
import akka.io.Tcp.{Bind, _}
import akka.io.{IO, Tcp}
import com.typesafe.scalalogging.Logger
import io.github.ankushs92.crdt.payload.Payload

// Actor to recieve partial computations from Windows

case class CRDTReplicaActor[T, U <: Payload[U], V](replica : CRDTReplica[T, U, V])(implicit system : ActorSystem) extends Actor {

  private val logger = Logger(this.getClass)
  private val socket = replica.getAddr

  IO(Tcp) ! Bind(self, replica.getAddr)

  logger.info(s"Initializing CRDTReplica TCP at socket : $socket")

  override def receive: Receive = {
    //Message emitted by a Window
//    case WindowPartialComputation(result : T) =>
//      logger.debug(s"Received message $result from Window")
//      replica.update(result)

    case Bound(localAddress) =>
      logger.info(s"CRDT Replica ready to accept connections at : $localAddress")

    case CommandFailed(cmd: Command) =>
      logger.error(s"Connection Failed : $cmd")
      context.stop(self)

    case Connected(remote, local) =>
      println("this worked")
      context.become {
        case Received(data) =>
          println("data")

        case PeerClosed =>
          println("aa")
          context.stop(self)
        case _ => println("se")
      }

  }
}

object T extends App {
//  val partial1 = new WindowPartialComputation[Double](5)
//  val partial2 = new WindowPartialComputation[Double](-3)
//  val localhost = new InetSocketAddress("localhost", 0)
//
//  val crdt1 = SummingStateCRDT(1, 10, localhost)
//  val system = ActorSystem("test")
//  val replicaActor = system.actorOf(Props(classOf[CRDTReplicaActor[Double, SummingPayload, Double]], crdt1, system))
//  replicaActor ! partial1
//  println(crdt1.getCurrentState)

}