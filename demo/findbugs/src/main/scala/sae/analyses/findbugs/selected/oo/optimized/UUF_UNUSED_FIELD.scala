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
package sae.analyses.findbugs.selected.oo.optimized

import sae.Relation
import sae.analyses.findbugs.base.oo.Definitions
import sae.bytecode._
import instructions.FieldReadInstruction
import sae.syntax.sql._
import structure.{FieldDeclaration, FieldInfo}
import sae.operators.impl.NotExistsInSameDomainView
import de.tud.cs.st.bat.resolved.{FieldType, ObjectType}

/**
 *
 * @author Ralf Mitschke
 *
 */

object UUF_UNUSED_FIELD
    extends (BytecodeDatabase => Relation[(ObjectType, String, FieldType)])
{
    def apply(database: BytecodeDatabase): Relation[(ObjectType, String, FieldType)] = {
        val definitions = Definitions (database)
        import database._
        import definitions._

        /*
        SELECT (*) FROM privateFields WHERE
            NOT (
                EXISTS (
                    SELECT (*) FROM readField WHERE
                        (((_: FieldReadInstruction).name) === ((_: FieldDeclaration).name)) AND
                        (((_: FieldReadInstruction).fieldType) === ((_: FieldDeclaration).fieldType)) AND
                        (((_: FieldReadInstruction).receiverType) === declaringType)
                )
            )
            */

        //new NotExistsInSameDomainView[FieldInfo](privateFields.asInstanceOf[Relation[FieldInfo]].asMaterialized, readField.asInstanceOf[Relation[FieldInfo]].asMaterialized)

        val privateFieldProjection: Relation[(ObjectType, String, FieldType)] = compile (SELECT ((fd: FieldDeclaration) => (fd.declaringType, fd.name, fd.fieldType)) FROM privateFields).forceToSet
        val readFieldProjection: Relation[(ObjectType, String, FieldType)] = compile (SELECT ((fd: FieldReadInstruction) => (fd.receiverType, fd.name, fd.fieldType)) FROM readField)

        new NotExistsInSameDomainView[(ObjectType, String, FieldType)](privateFieldProjection.asMaterialized, readFieldProjection.asMaterialized)
    }
}
