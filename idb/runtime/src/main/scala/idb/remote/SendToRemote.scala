package idb.remote

import akka.actor.ActorRef
import idb.observer.Observer

/**
 * An observer that forwards all events as messages to a possibly
 * remote actor.
 *
 * Later `SendToRemote` will need to have a `Pickler[Domain]`, so that
 * it can pickle values before they are sent to the `actor`.
 */
class SendToRemote[Domain](actor: ActorRef) extends Observer[Domain] {
  override def added(v: Domain) = actor ! Added(v)
  override def removed(v: Domain) = actor ! Removed(v)
  override def updated(oldV: Domain, newV: Domain) = actor ! Updated(oldV, newV)
  override def addedAll(vs: Seq[Domain]) = actor ! AddedAll(vs)
  override def removedAll(vs: Seq[Domain]) = actor ! RemovedAll(vs)
  override def endTransaction() = actor ! EndTransaction
}