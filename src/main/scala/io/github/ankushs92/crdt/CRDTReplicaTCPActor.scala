package io.github.ankushs92.crdt

import java.net.InetSocketAddress
import java.nio.ByteBuffer

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.io.Tcp.{Bind, _}
import akka.io.{IO, Tcp}
import akka.util.ByteString
import com.typesafe.scalalogging.Logger
import io.github.ankushs92.crdt.DeltaBased.{GCounterDeltaCRDT, GrowOnlySetDeltaCRDT, SummingDeltaCRDT}
import io.github.ankushs92.crdt.DeltaBased.buffers.{DeltaBuffer, DeltaBufferAdd, GCounterDeltaGroup, SummingDeltaGroup}
import io.github.ankushs92.crdt.payload.{GCounterPayload, GrowOnlySetPayload, Payload, SummingPayload}
import io.github.ankushs92.crdt.serializer.JsonSerde
import io.github.ankushs92.crdt.util.Json

//Use system's dispatcher as ExecutionContext
import scala.concurrent.duration._


case class CRDTReplicaActor[StateType,
  DeltaBufferQueryType,
  DeltaPayload <: Payload,
  DeltaBufferType <: DeltaBuffer[Int, DeltaBufferAdd, DeltaBufferQueryType],
  State,
  QueryResult]
(replica : DeltaCRDTReplica[StateType, DeltaBufferQueryType, DeltaPayload, DeltaBufferType, State, QueryResult])
(implicit system : ActorSystem)
  extends Actor {

  private val logger = Logger(this.getClass)
  private val socket = replica.getAddr
  import system.dispatcher


  IO(Tcp) ! Bind(self, socket)

  logger.debug(s"Initializing CRDTReplica TCP Server at socket : $socket")

  override def receive: Receive = {
    //Message emitted by a Window
    case WindowPartialComputation(aggType : AggType.Value, data) =>
      aggType match {
        case AggType.SET =>
          val write = data.toInt
          replica.asInstanceOf[GrowOnlySetDeltaCRDT[Int]].update(write)
        case AggType.COUNT =>
          val write = data.toInt
          replica.asInstanceOf[GCounterDeltaCRDT].update(write)
        case AggType.SUM =>
          val write = data.toDouble
          replica.asInstanceOf[SummingDeltaCRDT].update(write)
      }

    case Bound(localAddress) =>
      logger.info(s"CRDT TCP Server bounded to address : $localAddress")

      //Now that the tcp server is up and running, time to connect to other replicas and schedule periodic syncing between them
      val neighbours = replica.getNeighbours
      val syncIntervalMs = replica.getSyncInterval
      neighbours.foreach { neighbour =>
        val nbrReplicaId = neighbour.id
        val replicaTcpClient = system.actorOf(Props(new CRDTReplicaTCPClientActor(neighbour, self)))
        val periodicSyncActor = system.actorOf(Props(new CRDTReplicaPeriodicSyncActor(replicaTcpClient)))
        val replicate = Replicate(replica.getName, replica.getReplicaId, nbrReplicaId, replica)
        system.scheduler.scheduleWithFixedDelay(Duration.Zero, syncIntervalMs.millis, periodicSyncActor, replicate)
      }


    case CommandFailed(cmd: Command) =>
      logger.error(s"Connection Failed : $cmd")
      context.stop(self)

    case Connected(remote, local) =>
      logger.info(s"CRDTReplica TCP Server Ready to accept connections at addr $socket")
      val handler = context.actorOf(Props(CRDTReplicaTcpConnHandlerActor(replica.getAddr, self)))
      sender ! Register(handler)

    case data : ByteString =>
      val json = data.utf8String

      val payload = Json.toObject[SummingPayload](json)
      println("Merging " + payload + " being executed at replica " + replica.getReplicaId + " from neighrbour +: " + payload.getNbrReplicaId)

      replica.asInstanceOf[SummingDeltaCRDT].merge(payload)
      println("Replica " + replica.getReplicaId + " state " + replica.asInstanceOf[SummingDeltaCRDT].getCurrentState)
      println("Replica " + replica.getReplicaId + " delta buffer " + replica.asInstanceOf[SummingDeltaCRDT].getDeltaBuffer)
      println("Replica " + replica.getReplicaId + " Query result " + replica.asInstanceOf[SummingDeltaCRDT].query)


    case ReplicaAck(crdtType, replicaId, deltaGroup) =>
      crdtType match {
        case CRDTTypes.G_COUNTER =>
          replica.asInstanceOf[GCounterDeltaCRDT].replicaAck(replicaId, deltaGroup.asInstanceOf[Iterable[GCounterDeltaGroup]])

        case CRDTTypes.G_SET =>
          replica.asInstanceOf[GrowOnlySetDeltaCRDT[Int]].replicaAck(replicaId, deltaGroup.asInstanceOf[Iterable[Int]])

        case CRDTTypes.SUM =>
          replica.asInstanceOf[SummingDeltaCRDT].replicaAck(replicaId, deltaGroup.asInstanceOf[Iterable[SummingDeltaGroup]])
      }
  }
}


case class CRDTReplicaPeriodicSyncActor(tcpClient : ActorRef) extends Actor {

  override def receive: Receive = {

    case Replicate(crdtType, replicaId,  nbrReplicaId, replica) =>
      val deltaGroup = replica.getDeltaBuffer.bpAndccOptimization(nbrReplicaId)
      if(deltaGroup.nonEmpty) {
        println("Query result for replica " + replica.getReplicaId + " before sending is " + replica.asInstanceOf[SummingDeltaCRDT].query)
        tcpClient ! FilteredDeltaGroup(crdtType, replicaId, nbrReplicaId, deltaGroup)
      }
  }
}


case class Replicate[StateType,
                    DeltaBufferQueryType,
                    DeltaPayload <: Payload,
                    DeltaBufferType <: DeltaBuffer[Int, DeltaBufferAdd, DeltaBufferQueryType],
                    State,
                    QueryResult]
                    (crdtType : String,
                     replicaId : Int,
                     nbrReplicaId : Int,
                     replica : DeltaCRDTReplica[StateType, DeltaBufferQueryType, DeltaPayload, DeltaBufferType, State, QueryResult])

case class FilteredDeltaGroup[T](crdtType : String,
                                 replicaId : Int,
                                 nbrReplicaId : Int,
                                 values : Iterable[T])

case class ReplicaAck[T](crdtType : String, nbrReplicaId : Int, data : Iterable[T]) extends Tcp.Event

case class CRDTReplicaTCPClientActor(remote : Neighbour, replica : ActorRef) extends Actor {
  private def getBytes[T](t : T)(implicit m: Manifest[T]): ByteBuffer = ByteBuffer.wrap(new JsonSerde[T].serialize(t).getBytes)

  import Tcp._
  import context.system

  IO(Tcp) ! Connect(remote.addr)
  private val logger = Logger(this.getClass)

  override def receive = {
    case CommandFailed(_: Connect) =>
      logger.error(s"Could not establish a connection with $remote")
      context.stop(self)

    case c @ Connected(remote, local) =>
      val connection = sender()
      logger.debug(s"CRDT TCP Client $local ready to send messages to remote $remote")

      connection ! Register(self)
      context.become {

        case FilteredDeltaGroup(crdtType, replicaId, nbrReplicaId, deltaGroup) =>
          val bytes = crdtType match {
            case CRDTTypes.G_SET =>
              val growOnlySetPayload = GrowOnlySetPayload(replicaId, crdtType, deltaGroup.toSet)
              getBytes(growOnlySetPayload)

            case CRDTTypes.G_COUNTER =>
              val gCounterPayload = GCounterPayload(replicaId, crdtType, deltaGroup.asInstanceOf[Iterable[GCounterDeltaGroup]])
              getBytes(gCounterPayload)

            case CRDTTypes.SUM =>
              val summingPayload = SummingPayload(replicaId, crdtType, deltaGroup.asInstanceOf[Iterable[SummingDeltaGroup]])
              getBytes(summingPayload)

          }
          connection ! Write(ByteString(bytes), ReplicaAck(crdtType, nbrReplicaId, deltaGroup))

        case ack @ ReplicaAck(_, _, _) =>
          replica ! ack

        case CommandFailed(cmd : Command) =>
          logger.error(s"Error while writing to remote $remote : $cmd")

        case _: ConnectionClosed =>
          context.stop(self)

      }

  }}


case class CRDTReplicaTcpConnHandlerActor(selfAddr : InetSocketAddress, actorRef : ActorRef)
                                         (implicit system : ActorSystem) extends Actor {

  private val logger = Logger(this.getClass)

  import Tcp._

  def receive = {
    case Received(data) =>
      actorRef ! data

    case Close =>
      logger.info(s"Peer closed $selfAddr")
      context.stop(self)
  }
}




object Summing extends App {

  implicit val ac = ActorSystem("GlobalActorSystem")
  val addr1 = Neighbour(1, new InetSocketAddress(8081))
  val addr2 = Neighbour(2, new InetSocketAddress(8082))
  val addr3 = Neighbour(3, new InetSocketAddress(8083))

    val crdt1 = SummingDeltaCRDT(1, 2000, new InetSocketAddress(8081) , List(addr2, addr3))
//  val crdt1 = SummingDeltaCRDT(1, 4000, new InetSocketAddress(8081) , List(addr2))

  val crdt1Actor = ac.actorOf(Props(CRDTReplicaActor(crdt1)))


    val crdt2 = SummingDeltaCRDT(2, 2000, new InetSocketAddress(8082), List(addr1, addr3))
//  val crdt2 = SummingDeltaCRDT(2, 4000, new InetSocketAddress(8082), List(addr1))
  val crdt2Actor = ac.actorOf(Props( CRDTReplicaActor(crdt2)))

    val crdt3 = SummingDeltaCRDT(3, 2000, new InetSocketAddress(8083) , List(addr1, addr2))
    val crdt3Actor = ac.actorOf(Props( CRDTReplicaActor(crdt3)))
  //

  val replica1 = ac.actorOf(Props( TcpServer(new InetSocketAddress(8071), crdt1Actor)))
  val replica2 = ac.actorOf(Props( TcpServer(new InetSocketAddress(8072), crdt2Actor)))
    val replica3 = ac.actorOf(Props(new TcpServer(new InetSocketAddress(8073), crdt3Actor)))


}
//


//object t extends App {
//
//  //  replicaId : Int,
//  //  syncInterval : Int,
//  //  addr : InetSocketAddress,
//  //  neighbours : List[InetSocketAddress]
//  implicit val ac = ActorSystem("GlobalActorSystem")
//  val addr1 = Neighbour(1, new InetSocketAddress(8081))
//  val addr2 = Neighbour(2, new InetSocketAddress(8082))
//  val addr3 = Neighbour(3, new InetSocketAddress(8083))
//
////  val crdt1 = GCounterDeltaCRDT(1, 2000, new InetSocketAddress(8081) , List(addr2, addr3))
//    val crdt1 = GCounterDeltaCRDT(1, 4000, new InetSocketAddress(8081) , List(addr2))
//
//  val crdt1Actor = ac.actorOf(Props(new CRDTReplicaActor(crdt1)))
////
////
////  val crdt2 = GCounterDeltaCRDT(2, 2000, new InetSocketAddress(8082), List(addr1, addr3))
//    val crdt2 = GCounterDeltaCRDT(2, 4000, new InetSocketAddress(8082), List(addr1))
//  val crdt2Actor = ac.actorOf(Props(new CRDTReplicaActor(crdt2)))
////
////  val crdt3 = GCounterDeltaCRDT(3, 2000, new InetSocketAddress(8083) , List(addr1, addr2))
////  val crdt3Actor = ac.actorOf(Props(new CRDTReplicaActor(crdt3)))
//////
////
//  val replica1 = ac.actorOf(Props(new TcpServer(new InetSocketAddress(8071), crdt1Actor)))
//  val replica2 = ac.actorOf(Props(new TcpServer(new InetSocketAddress(8072), crdt2Actor)))
////  val replica3 = ac.actorOf(Props(new TcpServer(new InetSocketAddress(8073), crdt3Actor)))
//
//
//}

////Launch three replicas of GrowOnlySetDeltaCRDT on 3 random ports and start sending messages
////Define the three replicas to have a sync interval and they are all each others neighbours, and using
////3 command lines send messages to the three replicas seprately
//object t extends App {
//
//  //  replicaId : Int,
//  //  syncInterval : Int,
//  //  addr : InetSocketAddress,
//  //  neighbours : List[InetSocketAddress]
//  implicit val ac = ActorSystem("GlobalActorSystem")
//  val addr1 = Neighbour(1, new InetSocketAddress(8081))
//  val addr2 = Neighbour(2, new InetSocketAddress(8082))
//  val addr3 = Neighbour(3, new InetSocketAddress(8083))
//
//  val crdt1 = GrowOnlySetDeltaCRDT[Int](1, 2000, new InetSocketAddress(8081) , List(addr2, addr3))
////  val crdt1 = GrowOnlySetDeltaCRDT[Int](1, 4000, new InetSocketAddress(8081) , List(addr2))
//
//  val crdt1Actor = ac.actorOf(Props(new CRDTReplicaActor(crdt1)))
//
//
//  val crdt2 = GrowOnlySetDeltaCRDT[Int](2, 2000, new InetSocketAddress(8082), List(addr1, addr3))
////  val crdt2 = GrowOnlySetDeltaCRDT[Int](2, 4000, new InetSocketAddress(8082), List(addr1))
//  val crdt2Actor = ac.actorOf(Props(new CRDTReplicaActor(crdt2)))
//
//  val crdt3 = GrowOnlySetDeltaCRDT[Int](3, 2000, new InetSocketAddress(8083) , List(addr1, addr2))
//  val crdt3Actor = ac.actorOf(Props(new CRDTReplicaActor(crdt3)))
//
//
//  val replica1 = ac.actorOf(Props(new TcpServer(new InetSocketAddress(8071), crdt1Actor)))
//  val replica2 = ac.actorOf(Props(new TcpServer(new InetSocketAddress(8072), crdt2Actor)))
//  val replica3 = ac.actorOf(Props(new TcpServer(new InetSocketAddress(8073), crdt3Actor)))
//
//
//}



case class TcpServer(addr : InetSocketAddress, actorRef : ActorRef)(implicit val ac : ActorSystem) extends Actor {
  IO(Tcp) ! Bind(self, addr)

  override def receive: Receive = {
    case Bound(localAddress) =>
      println(s"CRDT bounded to address : $localAddress")

    case CommandFailed(cmd: Command) =>
      println(s"Connection Failed : $cmd")
      context.stop(self)

    case Connected(remote, local) =>
      println(s"CRDTReplica Ready to accept connections at addr $addr")
      sender ! Register(self)
      context.become {
        case Received(data) =>
          //          val preAggregateSet = WindowPartialComputation(SET, data.utf8String.trim.toInt.toByte)
          //          val preAggregateCount = WindowPartialComputation(COUNT, 1.toByte)
          val preAggregateSum = WindowPartialComputation(AggType.SUM, data.utf8String.trim.toDouble.toByte)
          actorRef ! preAggregateSum

        case Close     =>
          context.stop(self)
      }
  }
}
