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
package idb.algebra.demo

import idb.algebra.ir.{RelationalAlgebraIRBasicOperators, RelationalAlgebraIRRemoteOperators}
import idb.lms.extensions.{FunctionUtils, ScalaOpsPkgExpExtensions}
import idb.lms.extensions.operations.{OptionOpsExp, SeqOpsExpExt, StringOpsExpExt}
import idb.lms.extensions.print.{CodeGenIndent, QuoteFunction}

import scala.virtualization.lms.common.{ScalaOpsPkgExp, StaticDataExp, StructExp, TupledFunctionsExp}

/**
 *
 * @author Ralf Mitschke
 */
trait RelationalAlgebraDemoPrintPlanRemoteOperators
    extends RelationalAlgebraDemoPrintPlanBase
    with QuoteFunction
    with CodeGenIndent
{

	override val IR: ScalaOpsPkgExpExtensions with
        FunctionUtils with RelationalAlgebraIRRemoteOperators


    import IR.{Remote, Def, Exp, Reclassification, ActorDef, Declassification}


    override def quoteRelation (x: Exp[Any]): String =
        x match {
            case Def (rel@Remote (relation, newHost)) =>
                withIndent (s"remote[${rel.host.name}<--](\n") +
                    withMoreIndent (quoteRelation (relation) + "\n") +
                    withIndent (")")

            case Def (rel@Reclassification(r, newTaint)) =>
				withIndent (s"reclass[${rel.host.name}](taint = ${newTaint.toString}\n") +
					withMoreIndent (quoteRelation (r) + "\n") +
					withIndent (")")

            case Def (rel@Declassification(r, taints)) =>
	            withIndent (s"declass[${rel.host.name}](\n") +
		            withMoreIndent (quoteRelation (r) + "\n") +
		            withMoreIndent (taints.toString + "\n") +
		            withIndent (")")

            case Def (rel@ActorDef (path, host, taint)) =>
	            withIndent (s"actor[${host.name}](${path.toString})")

            case _ => super.quoteRelation (x)
        }

}
