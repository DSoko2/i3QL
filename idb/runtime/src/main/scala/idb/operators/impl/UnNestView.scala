/* License (BSD Style License):
 *  Copyright (c) 2009, 2011
 *  Software Technology Group
 *  Department of Computer Science
 *  Technische Universität Darmstadt
 *  All rights reserved.
 *
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are met:
 *
 *  - Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  - Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *  - Neither the name of the Software Technology Group or Technische
 *    Universität Darmstadt nor the names of its contributors may be used to
 *    endorse or promote products derived from this software without specific
 *    prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.
 */
package idb.operators.impl

import idb.Relation
import idb.observer.{Observable, NotifyObservers, Observer}
import idb.operators.UnNest


class UnNestView[Domain, Range](
                                 val relation: Relation[Domain],
                                 val unNestFunction: Domain => Traversable[Range],
                                 override val isSet: Boolean
                                 )
  extends UnNest[Domain, Range]
  with Observer[Domain]
  with NotifyObservers[(Domain, Range)] {

  relation.addObserver(this)


  override protected def childObservers(o: Observable[_]): Seq[Observer[_]] = {
    if (o == relation) {
      return List(this)
    }
    Nil
  }

  override protected def resetInternal(): Unit = {

  }


  /**
   * Applies f to all elements of the view.
   */
  def foreach[T](f: ((Domain, Range)) => T) {
    relation.foreach((v: Domain) =>
      unNestFunction(v).foreach((u: Range) =>
        f((v, u))
      )
    )
  }


  def updated(oldV: Domain, newV: Domain) {
    removed(oldV)
    added(newV)
  }

  def unnestPairs(v: Domain): Seq[(Domain, Range)] =
    unNestFunction(v).toSeq map (r => (v,r))

  def removed(v: Domain) {
    val removed = unnestPairs(v)
    notify_removedAll(removed)
  }

  def removedAll(vs: Seq[Domain]) {
    val removed = vs.foldLeft(Seq[(Domain,Range)]())((seq,v) => seq ++ unnestPairs(v))
    notify_removedAll(removed)
  }

  def added(v: Domain) {
    val added = unnestPairs(v)
    notify_addedAll(added)
  }

  def addedAll(vs: Seq[Domain]) {
    val added = vs.foldLeft(Seq[(Domain,Range)]())((seq,v) => seq ++ unnestPairs(v))
    notify_addedAll(added)
  }
}

object UnNestView {
  def apply[Domain, Range](
                            relation: Relation[Domain],
                            unNestFunction: Domain => Traversable[Range],
                            isSet: Boolean
                            ): UnNestView[Domain, Range] = {
    new UnNestView[Domain, Range](relation, unNestFunction, isSet)
  }

}
