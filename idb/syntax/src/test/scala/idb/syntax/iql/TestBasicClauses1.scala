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
package idb.syntax.iql

import TestUtil.assertEqualStructure
import UniversityDatabase._
import idb.schema.university._
import idb.syntax.iql.IR._
import org.junit.{Ignore, Test}
import scala.language.implicitConversions


/**
 *
 * @author Ralf Mitschke
 */
class TestBasicClauses1
{
    @Test
    def testExtent () {
        val query = plan (
            SELECT (*) FROM students
        )

        assertEqualStructure (extent (students), query)
    }

    @Test
    def testProject1 () {
        val query = plan (
            SELECT ((_: Rep[Student]).lastName) FROM students
        )

        assertEqualStructure (
            projection (extent (students), (_: Rep[Student]).lastName),
            query
        )
    }

    @Test
    def testProject1TupleFun () {
        val query = plan (
            //SELECT (firstName, lastName) FROM students // TODO maybe re-enable that in a later version
            SELECT ((s: Rep[Student]) => (s.firstName, s.lastName)) FROM students
        )

        assertEqualStructure (
            projection (extent (students), fun ((s: Rep[Student]) => (s.firstName, s.lastName))),
            query
        )
    }


    @Test
    def testSelection1 () {
        val query = plan (
            SELECT (*) FROM students WHERE ((s: Rep[Student]) => s.firstName == "Sally")
        )

        assertEqualStructure (
            selection (extent (students), (s: Rep[Student]) => s.firstName == "Sally"),
            query
        )
    }

    @Test
    def testSelection1Negate () {
        val query = plan (
            SELECT (*) FROM students WHERE ((s: Rep[Student]) => s.lastName == "Fields" && !(s.firstName == "Sally"))
        )

        assertEqualStructure (
            selection (
                selection (extent (students), (s: Rep[Student]) => s.lastName == "Fields"),
                (s: Rep[Student]) => !(s.firstName == "Sally")
            ),
            query
        )
    }

	//TODO why does this fail?
    @Test
	@Ignore
    def testSelection1NegateMultiplePositive () {
        val query = plan (
            SELECT (*) FROM students WHERE ((s: Rep[Student]) => s.lastName == "Fields" && s.matriculationNumber > 0 && !(s.firstName == "Sally"))
        )

        assertEqualStructure (

            selection (
                selection(
                    selection (extent (students), (s: Rep[Student]) => s.lastName == "Fields"),
                    (s: Rep[Student]) => s.matriculationNumber > 0
                ),
                (s: Rep[Student]) => !(s.firstName == "Sally")
            ),
            query
        )
    }

    @Test
    def testSelection1FunCall () {
        val query = plan (
            SELECT (*) FROM courses WHERE ((c: Rep[Course]) => c.title.startsWith ("Introduction"))
        )

        assertEqualStructure (
            selection (extent (courses), (c: Rep[Course]) => c.title.startsWith ("Introduction")),
            query
        )
    }

    @Test
    def testSelection1ProjectTupleFun () {
        val query = plan (
            //SELECT (firstName, lastName) FROM students WHERE ((s: Rep[Student]) => s.firstName == "Sally")
            SELECT ((s: Rep[Student]) => (s.firstName, s.lastName)) FROM students WHERE (
                (s: Rep[Student]) => s.firstName == "Sally"
                )
        )

        assertEqualStructure (
            projection (
                selection (
                    extent (students),
                    fun ((s: Rep[Student]) => s.firstName == "Sally")
                ),
                fun ((s: Rep[Student]) => (s.firstName, s.lastName))
            ),
            query
        )
    }


    @Test
    def testSelection1ProjectTupleDirect () {
        val query = plan (
            SELECT ((s: Rep[Student]) => (s.firstName, s.lastName)) FROM students WHERE ((s: Rep[Student]) => s
                .firstName == "Sally")
        )

        assertEqualStructure (
            projection (
                selection (
                    extent (students),
                    fun ((s: Rep[Student]) => s.firstName == "Sally")
                ),
                fun ((s: Rep[Student]) => (s.firstName, s.lastName))
            ),
            query
        )
    }


    @Test
    def testDistinctExtent () {
        val query = plan (
            SELECT DISTINCT (*) FROM students
        )

        assertEqualStructure (
            duplicateElimination (
                extent (students)
            ),
            query
        )
    }

    @Ignore
    @Test
    def testAggregateCountStar () {
        val query = plan (
            SELECT (COUNT (*)) FROM students
        )


    }

    @Ignore
    @Test
    def testAggregateSumCreditPoints () {
        val query = plan (
            SELECT (SUM (creditPoints)) FROM courses
        )


    }

    @Test
    def testAggregateGroup1 () {

        val query = plan (
            SELECT ((s: Rep[String]) => s) FROM students GROUP BY (_.lastName)
        )

        /*	assertEqualStructure(
                projection (
                    grouping (
                        extent (students),
                        fun ((s : Rep[Student]) => s.lastName)
                    ),
                    fun ((s: Rep[String]) => s)
                ),
                query
            )    */

    }

    @Test
    def testAggregateGroup2 () {

        val query = plan (
            SELECT (
                //	(firstName : Rep[String], lastName : Rep[String]) => firstName + " " + lastName  //TODO Re-enable
                // in later version
                (pair: Rep[(String, String)]) => pair._1 + " " + pair._2
            ) FROM students GROUP BY ((s: Rep[Student]) => (s.firstName, s.lastName))
        )

    }

    @Ignore
    @Test
    def testAggregateGroupCountStar () {
        val query = plan (
            SELECT (COUNT (*)) FROM students GROUP BY (_.lastName)
        )
    }

    @Ignore
    @Test
    def testAggregateGroupCountStarWithGroup () {
        val query = plan (
            SELECT ((s: Rep[String]) => s, COUNT (*)) FROM students GROUP BY (_.lastName)
        )
    }

    @Ignore
    @Test
    def testAggregateGroupWithWhere () {

        val query = plan (
            SELECT ((s: Rep[String]) => s) FROM students WHERE (_.matriculationNumber > 10000) GROUP BY (_.lastName)
        )

    }

    @Ignore
    @Test
    def testAggregateGroupCountStarWithWhere () {
        val query = plan (
            SELECT (COUNT (*)) FROM students WHERE (_.matriculationNumber > 10000) GROUP BY (_.lastName)
        )
    }

    @Ignore
    @Test
    def testAggregateGroupCountStarWithGroupWithWhere () {
        val query = plan (
            SELECT ((s: Rep[String]) => s, COUNT (*)) FROM students WHERE (_.matriculationNumber > 10000) GROUP BY (
                _.lastName)
        )
    }

}
