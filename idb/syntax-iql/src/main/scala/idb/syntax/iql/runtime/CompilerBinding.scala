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
package idb.syntax.iql.runtime

import akka.actor.{ActorPath, ActorSystem}
import idb.Relation
import idb.algebra.compiler._
import idb.algebra.compiler.boxing.{BoxedAggregationNotSelfMaintained, BoxedAggregationSelfMaintained, BoxedEquiJoin, BoxedFunction}
import idb.lms.extensions.ScalaCodeGenPkgExtensions
import idb.operators.impl.{ProjectionView, SelectionView, UnNestView}
import idb.syntax.iql

import scala.language.postfixOps

/**
 *
 * @author Ralf Mitschke
 */
case object CompilerBinding
    extends RelationalAlgebraGenBasicOperatorsAsIncremental
    with RelationalAlgebraGenSetTheoryOperatorsAsIncremental
    with RelationalAlgebraGenAggregationOperatorsAsIncremental
    with RelationalAlgebraGenRecursiveOperatorsAsIncremental
	with RelationalAlgebraGenRemoteOperatorsAsIncremental
    with RelationalAlgebraGenCacheAll
    with ScalaCodeGenPkgExtensions
{
    override val IR = iql.IR

    override def reset {
      resetQueryCache()
      IR.reset
      super.reset
    }

	override def remoteRelation[Domain](path: ActorPath): Relation[Domain] =
		RemoteUtils.receiver[Domain](path)

	override def remoteDeploy[Domain](system: ActorSystem, rel: Relation[Domain], path: ActorPath): Relation[Domain] = {
		val controllerRef = RemoteUtils.deployOperator(system, path)(rel)
		RemoteUtils.receiver[Domain](controllerRef)
	}

	/**
	  *
	  * @param relation
	  * @param recursion If set to false, the children of the relation won't be handled recursively
	  */
	def initialize(relation: Relation[_], recursion: Boolean = true): Unit = {
		relation match {
			case r : SelectionView[_] =>
				BoxedFunction.compile(r.filter, CompilerBinding)

			case r : ProjectionView[_, _] =>
				BoxedFunction.compile(r.projection, CompilerBinding)

			case r : BoxedEquiJoin[_, _] =>
				r.compile(CompilerBinding)

			case r : BoxedAggregationSelfMaintained[_, _, _, _, _] =>
				r.compile(CompilerBinding)

			case r : BoxedAggregationNotSelfMaintained[_, _, _, _, _] =>
				r.compile(CompilerBinding)

			case r : UnNestView[_, _] =>
				BoxedFunction.compile(r.unNestFunction, CompilerBinding)

			case _ =>
		}

		if (recursion)
			relation.children.foreach(c => initialize(c))
	}

	def unboxRelation(relation: Relation[_]): Relation[_] =
		relation match {
			case r: BoxedEquiJoin[_, _] =>
				r.equiJoin
			case r: BoxedAggregationSelfMaintained[_, _, _, _, _] =>
				r.aggregation
			case r: BoxedAggregationNotSelfMaintained[_, _, _, _, _] =>
				r.aggregation
			case _ => relation
		}
}
