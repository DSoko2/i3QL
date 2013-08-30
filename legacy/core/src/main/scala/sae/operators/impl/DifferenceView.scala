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
package sae.operators.impl

import sae.operators.Difference
import sae.{MaterializedRelation, Relation, Observable, Observer}
import sae.deltas.{Update, Deletion, Addition}

/**
 * The difference operation in our algebra has non-distinct bag semantics
 *
 * This class can compute the difference efficiently by relying on indices from the underlying relations.
 * The operation itself does not store any intermediate results.
 * Updates are computed based on indices and foreach is recomputed on every call.
 *
 *
 * The difference can be update by the expression:
 * [(Δright- ∪ Δleft+) - (Δleft- ∪ Δright+)] - (right - left)
 */
class DifferenceView[Domain](val left: Relation[Domain],
                             val right: Relation[Domain])
    extends Difference[Domain] with MaterializedRelation[Domain]
{
    left addObserver LeftObserver

    right addObserver RightObserver

    import com.google.common.collect.HashMultiset

    private val leftDiffRight: HashMultiset[Domain] = HashMultiset.create[Domain]()

    private val rightDiffLeft: HashMultiset[Domain] = HashMultiset.create[Domain]()

    lazyInitialize ()


    override protected def childObservers(o: Observable[_]): Seq[Observer[_]] = {
        if (o == left) {
            return List (LeftObserver)
        }
        if (o == right) {
            return List (RightObserver)
        }
        Nil
    }

    /**
     * Each view must be able to
     * materialize it's content from the underlying
     * views.
     * The laziness allows a query to be set up
     * on relations (tables) that are already filled.
     * The lazy initialization must be performed prior to processing the
     * first add/delete/update events or foreach calls.
     */
    def lazyInitialize() {
        val intersection: HashMultiset[Domain] = HashMultiset.create[Domain]()
        left.foreach (v => {
            leftDiffRight.add (v)
            intersection.add (v)
        })
        right.foreach (v => rightDiffLeft.add (v))
        intersection.retainAll (rightDiffLeft)
        leftDiffRight.removeAll (intersection)
        rightDiffLeft.removeAll (intersection)
    }

    /**
     * Applies f to all elements of the view with their counts
     */
    def foreachWithCount[T](f: (Domain, Int) => T) {}

    def isDefinedAt(v: Domain) = false

    /**
     * Returns the count for a given element.
     * In case an add/remove/update event is in progression, this always returns the
     */
    def elementCountAt[T >: Domain](v: T) = 0

    /**
     * Applies f to all elements of the view.
     */
    def foreach[T](f: (Domain) => T) {
        val it = leftDiffRight.iterator ()
        while (it.hasNext) {
            val v = it.next ()
            f (v)
        }
    }


    /**
     * [(Δright- ∪ Δleft+) - (Δleft- ∪ Δright+)] - (right - left)
     */
    object LeftObserver extends Observer[Domain]
    {

        override def endTransaction() {
            notifyEndTransaction ()
        }

        /**
         * Δleft+ - (right - left)
         */
        def added(v: Domain) {
            if (rightDiffLeft.count (v) == 0) {
                leftDiffRight.add (v)
                element_added (v)
            }
            else
            {
                rightDiffLeft.remove (v)
            }
        }

        /**
         * - Δleft-  - (right - left)
         */
        def removed(v: Domain) {
            if (leftDiffRight.count (v) > 0) {
                leftDiffRight.remove (v)
                element_removed (v)
            }
            else
            {
                // if it was not in the leftDiffRight newResult it was filtered by being in right
                rightDiffLeft.add (v)
            }
        }

        def updated(oldV: Domain, newV: Domain) {
            var count = leftDiffRight.count (oldV) + rightDiffLeft.count (oldV)
            if (count == 0) {
                added (newV)
            }
            while (count > 0)
            {
                removed (oldV)
                added (newV)
                count -= 1
            }
        }

        def updated[U <: Domain](update: Update[U]) {
            throw new UnsupportedOperationException
        }

        def modified[U <: Domain](additions: Set[Addition[U]], deletions: Set[Deletion[U]], updates: Set[Update[U]]) {
            throw new UnsupportedOperationException
        }
    }

    object RightObserver extends Observer[Domain]
    {
        override def endTransaction() {
            notifyEndTransaction ()
        }

        def added(v: Domain) {
            if (leftDiffRight.count (v) > 0) {
                leftDiffRight.remove (v)
                element_removed (v)
            }
            else
            {
                rightDiffLeft.add (v)
            }
        }

        def removed(v: Domain) {
            if (rightDiffLeft.count (v) == 0) {
                leftDiffRight.add (v)
                element_added (v)
            }
            else
            {
                rightDiffLeft.remove (v)
            }
        }

        def updated(oldV: Domain, newV: Domain) {
            var count = leftDiffRight.count (oldV) + rightDiffLeft.count (oldV)
            if (count == 0) {
                added (newV)
            }
            while (count > 0)
            {
                removed (oldV)
                added (newV)
                count -= 1
            }
        }

        def updated[U <: Domain](update: Update[U]) {
            throw new UnsupportedOperationException
        }

        def modified[U <: Domain](additions: Set[Addition[U]], deletions: Set[Deletion[U]], updates: Set[Update[U]]) {
            throw new UnsupportedOperationException
        }
    }


}