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
package idb.lms.extensions.operations

import scala.virtualization.lms.common.{ScalaGenSeqOps, SeqOpsExp, CastingOpsExp}
import scala.reflect.SourceContext

/**
 *
 * @author Ralf Mitschke
 */
trait SeqOpsExpExt
    extends SeqOpsExp
{

    override implicit def varToSeqOps[A:Manifest](x: Var[Seq[A]]) = new SeqOpsClsExt(readVar(x))
    override implicit def repSeqToSeqOps[T:Manifest](a: Rep[Seq[T]]) = new SeqOpsClsExt(a)
    override implicit def seqToSeqOps[T:Manifest](a: Seq[T]) = new SeqOpsClsExt(unit(a))

    class SeqOpsClsExt[T:Manifest](a: Rep[Seq[T]]) extends SeqOpsCls[T](a) {
        def head(implicit pos: SourceContext) = seq_head(a)
        def tail(implicit pos: SourceContext) = seq_tail(a)
    }

    case class SeqHead[T:Manifest](xs: Exp[Seq[T]]) extends Def[T]
    case class SeqTail[T:Manifest](xs: Exp[Seq[T]]) extends Def[Seq[T]]


    def seq_head[T:Manifest](xs: Exp[Seq[T]])(implicit pos: SourceContext): Exp[T] = SeqHead(xs)
    def seq_tail[T:Manifest](xs: Exp[Seq[T]])(implicit pos: SourceContext): Exp[Seq[T]] = SeqTail(xs)



    override def mirror[A: Manifest] (e: Def[A], f: Transformer)(implicit pos: SourceContext): Exp[A] = (e match {
        case SeqHead (xs) => seq_head (f (xs))
        case SeqTail (xs) => seq_tail (f (xs))
        case _ => super.mirror (e, f)
    }).asInstanceOf[Exp[A]]

}


trait ScalaGenSeqOpsExt
extends ScalaGenSeqOps
{
    val IR: SeqOpsExpExt
    import IR._

    override def emitNode(sym: Sym[Any], rhs: Def[Any]) = rhs match {
        case SeqHead(xs) => emitValDef(sym, quote(xs) + ".head")
        case SeqTail(xs) => emitValDef(sym, quote(xs) + ".tail")
        case _ => super.emitNode(sym, rhs)
    }
}