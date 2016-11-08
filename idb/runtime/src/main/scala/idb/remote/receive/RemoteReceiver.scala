package idb.remote.receive

import java.io.PrintStream

import akka.actor.{ActorRef, ActorSystem, Props}
import idb.Relation
import idb.observer.{NotifyObservers, Observer}
import idb.remote.{Print, Reset, SendTo}

import scala.language.postfixOps

/**
  * Created by mirko on 18.10.16.
  */
trait RemoteReceiver[Domain] extends Relation[Domain] with NotifyObservers[Domain] with Observer[Domain] {


	import scala.concurrent.duration._
	val timeout = 10 seconds

	private var receiveActorRef : ActorRef = null
	private var sendActorRef : ActorRef = null

	/**
	  * Creates links to the children actors.
	  */
	def deploy(system : ActorSystem): Unit

	protected def internalDeploy(system : ActorSystem, remoteRef : ActorRef): Unit = {
		if (sendActorRef != null) {
			Predef.println("[RemoteReceiver] Warning! RemoteReceiver will not be deployed: Already deployed.")
			return
		}

		sendActorRef = remoteRef
		receiveActorRef = system.actorOf(Props(classOf[ReceiveActorAdapter[Domain]], this))
		println(s"[RemoteReceiver] Adding link: ${sendActorRef.path} ---> ${receiveActorRef.path}")
		sendActorRef ! SendTo(receiveActorRef)

	}

	override def isSet: Boolean = false

	override def foreach[T](f: (Domain) => T): Unit = {}

	override def children: Seq[Relation[_]] = Nil

	override protected def resetInternal(): Unit = {
		sendActorRef ! Reset
	}

	override protected[idb] def printInternal(out : PrintStream)(implicit prefix: String = " "): Unit = {
		out.println(prefix + s"Receiver(${sendActorRef.path.toStringWithoutAddress} --> ${receiveActorRef.path.toStringWithoutAddress})")
		sendActorRef ! Print
	}

	override def updated(oldV: Domain, newV: Domain): Unit = {
		notify_updated(oldV, newV)
	}

	override def removed(v: Domain): Unit = {
		notify_removed(v)
	}

	override def removedAll(vs: Seq[Domain]): Unit = {
		notify_removedAll(vs)
	}

	override def added(v: Domain): Unit = {
		notify_added(v)
	}

	override def addedAll(vs: Seq[Domain]): Unit = {
		notify_addedAll(vs)
	}
}
