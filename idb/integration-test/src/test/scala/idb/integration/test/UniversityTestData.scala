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
package idb.integration.test

import idb.schema.university.{CoursePrerequisite, Registration, Course, Student}

/**
 *
 * @author Ralf Mitschke
 */
trait UniversityTestData
{
    val sallyFields = Student (1, "Sally", "Fields")

    val johnDoe = Student (2, "John", "Doe")

    val johnCarter = Student (3, "John", "Carter")

    val judyCarter = Student (4, "Judy", "Carter")

    val janeDoe = Student (5, "Jane", "Doe")

    val ics1 = Course (1, "Introduction to Computer Science I", 9)

    val ics2 = Course (2, "Introduction to Computer Science II", 9)

    val eise = Course (3, "Introduction to Software Engineering", 5)

    val sedc = Course (4, "Softare Design and Construction", 6)

    val introProgLang = Course (5, "Introdcution to Progamming Langauges", 6)

    val advancedProgLang = Course (6, "Advanced Concepts in Progamming Langauges", 6)

    val sallyTakesIcs1 = Registration (1, 1, "Sally takes ICS1")

    val ics2Prerequisites = List (CoursePrerequisite (ics2, ics1))

    val eisePrerequisites = List (CoursePrerequisite (eise, ics1))

    val sedcPrerequisites = List (CoursePrerequisite (sedc, eise))

    val introProgLangPrereqisites = List (
        CoursePrerequisite (introProgLang, ics1),
        CoursePrerequisite (introProgLang, ics2)
    )

    val advancedProgLangPrereqisites = List (CoursePrerequisite (advancedProgLang, introProgLang))
}