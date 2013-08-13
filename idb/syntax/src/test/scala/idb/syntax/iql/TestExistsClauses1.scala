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
import org.junit.{Ignore, Test}
import UniversityDatabase._
import idb.schema.university._
import idb.syntax.iql.IR._

import scala.language.implicitConversions

/**
 *
 * @author Ralf Mitschke
 */
class TestExistsClauses1
{

    @Ignore
    @Test
    def testExists () {
        val query = plan (
            SELECT (*) FROM students WHERE ((s: Rep[Student]) =>
                EXISTS (
                    SELECT (*) FROM registrations WHERE ((r: Rep[Registration]) =>
                        s.matriculationNumber == r.studentMatriculationNumber
                        )
                )
                )
        )

        assertEqualStructure (
            semiJoin (
                students,
                registrations,
                (s: Rep[Student]) => s.matriculationNumber,
                (r: Rep[Registration]) => r.studentMatriculationNumber
            ),
            query
        )

    }

    @Ignore
    @Test
    def testExistsWithConjunction () {
        val query = plan (
            SELECT (*) FROM students WHERE ((s: Rep[Student]) =>
                s.lastName == "Fields" AND NOT (s.firstName == "Sally") AND
                    EXISTS (
                        SELECT (*) FROM registrations WHERE ((r: Rep[Registration]) =>
                            s.matriculationNumber == r.studentMatriculationNumber
                            )
                    )
                )
        )

        assertEqualStructure (
            semiJoin (
                selection (
                    students,
                    (s: Rep[Student]) => s.lastName == "Fields" && !(s.firstName == "Sally")
                ),
                registrations,
                (s: Rep[Student]) => s.matriculationNumber,
                (r: Rep[Registration]) => r.studentMatriculationNumber
            ),
            query
        )

    }


    @Ignore
    @Test
    def testNotExists () {
        val query = plan (
            SELECT (*) FROM students WHERE ((s: Rep[Student]) =>
                s.lastName == "Fields" AND
                    NOT (s.firstName == "Sally") AND
                    NOT (EXISTS (
                        SELECT (*) FROM registrations WHERE ((r: Rep[Registration]) =>
                            s.matriculationNumber == r.studentMatriculationNumber
                            )
                    ))
                )
        )

    }
}
