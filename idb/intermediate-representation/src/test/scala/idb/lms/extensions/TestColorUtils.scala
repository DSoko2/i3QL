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
package idb.lms.extensions

import idb.lms.extensions.operations.StringOpsExpExt
import idb.query.colors._
import org.junit.Assert._
import org.junit.{Ignore, Test}

import scala.virtualization.lms.common._

/**
 *
 * @author Ralf Mitschke, Mirko Köhler
 */
class TestColorUtils
    extends BaseFatExp
    with NumericOpsExp
    with EffectExp
    with EqualExp
    with TupledFunctionsExp
    with TupleOpsExp
    with BooleanOpsExp
    with LiftAll
    with FunctionUtils
	with PrimitiveOpsExp
	with StringOpsExpExt
{


	@Test
	def testColorsInExp1(): Unit = {
		val variable : Exp[Int] = 0 + 1

		val exp = int_plus(
			field(
				field(variable, "_1"),
				"count"
			),
			1
		)

		val coloring = FieldColor(
			Map(
				FieldName("_1") -> FieldColor(Map(FieldName("count") -> Color("blue"))),
			    FieldName("_2") -> Color("yellow")
			)
		)

		println(colorsOfTFieldsInExp(exp, variable, coloring))



	}

	@Test
	def testColorsInFun1(): Unit = {
		val f = (t : Rep[((Int, Int), Int)]) => t._1._2 + 1

		val coloring = FieldColor(
			Map(
				FieldName("_1") -> FieldColor(Map(FieldName("_1") -> Color("red"), FieldName("_2") -> Color("blue"))),
				FieldName("_2") -> Color("yellow")
			)
		)

		assertEquals(Set(Color("blue")), colorsOfTFields(f, coloring))
	}


	@Test
	def testColorsInFun2(): Unit = {
		val f = (t : Rep[((Int, Int), Int)]) => t._1._2 + t._2

		val coloring = FieldColor(
			Map(
				FieldName("_1") -> FieldColor(Map(FieldName("_1") -> Color("red"), FieldName("_2") -> Color("blue"))),
				FieldName("_2") -> Color("yellow")
			)
		)

		assertEquals(Set(Color("blue"), Color("yellow")), colorsOfTFields(f, coloring))
	}

	@Test
	def testColorsInFun3(): Unit = {
		val f = (t : Rep[(Int, Int, String)]) => string_length(t._3) + t._1

		val c1 = Color("red")
		val c2 = Color("blue")

		assertEquals(Set(Color("blue"), Color("red")), colorsOfTFields(f, Color.tupled(c1, c1, c2)))
	}



}