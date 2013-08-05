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
package idb.algebra.opt

import idb.algebra.ir.RelationalAlgebraIRBasicOperators

/**
 *
 * @author Ralf Mitschke
 *
 */
trait RelationalAlgebraIROptPushSetTheoryOps
    extends RelationalAlgebraIRBasicOperators
{

    override def intersection[Domain: Manifest] (
        relationA: Rep[Query[Domain]],
        relationB: Rep[Query[Domain]]
    ): Rep[Query[Domain]] =
        ((relationA, relationB) match {
            // Π_f(a) ∩ Π_f(b) => Π_f(a ∩ b)
            case (Def (Projection (a, fa)), Def (Projection (b, fb))) if fa == fb =>
                projection (intersection (a, b)(domainOf (a)), fa)

               /*
            //  (a × b) ∩ (c × d) => (a ∩ c) × (b ∩ d)
            case (Def (CrossProduct (a, b)), Def (CrossProduct (c, d))) =>
                crossProduct (
                    intersection (a, c)(domainOf (a)),
                    intersection (b, d)(domainOf (b))
                )

            //  (a × b) ∩ (c ⋈ d) => (a ∩ c) ⋈ (b ∩ d)
            case (Def (CrossProduct (a, b)), Def (EquiJoin (c, d, l))) =>
                equiJoin (
                    intersection (a, c)(domainOf (a)),
                    intersection (b, d)(domainOf (b)),
                    l
                )

            //  (a ⋈ b) ∩ (c × d) => (a ∩ c) ⋈ (b ∩ d)
            case (Def (EquiJoin (a, b, l)), Def (CrossProduct (c, d))) =>
                equiJoin (
                    intersection (a, c)(domainOf (a)),
                    intersection (b, d)(domainOf (b)),
                    l
                )

            //  (a ⋈ b) ∩ (c ⋈ d) => (a ∩ c) ⋈ (b ∩ d)
            case (Def (EquiJoin (a, b, l1)), Def (EquiJoin (c, d, l2))) =>
                equiJoin (
                    intersection (a, c)(domainOf (a)),
                    intersection (b, d)(domainOf (b)),
                    l1 ::: l2
                )

            //  σ(a) ∩ b => σ(a ∩ b)
            case (Def (Selection (a, f)), b) if a != b =>
                selection (intersection (a, b)(exactDomainOf(a)), f)

            case (Def (Intersection (r, Def (CrossProduct (a, b)))), Def (CrossProduct (c, d))) =>
                intersection (
                    r.asInstanceOf[Rep[Query[Domain]]],
                    crossProduct (
                        intersection (a, c)(domainOf (a)),
                        intersection (b, d)(domainOf (b))
                    ).asInstanceOf[Rep[Query[Domain]]]
                )

            case (Def (Intersection (Def (EquiJoin (a, b, l)), r)), Def (CrossProduct (c, d))) =>
                intersection (
                    equiJoin (
                        intersection (a, c)(domainOf (a)),
                        intersection (b, d)(domainOf (b)),
                        l
                    ).asInstanceOf[Rep[Query[Domain]]],
                    r.asInstanceOf[Rep[Query[Domain]]]
                )
                       */

            case _ => super.intersection (relationA, relationB)


        }).asInstanceOf[Rep[Query[Domain]]]
}
