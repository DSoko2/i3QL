package unisson.model.kinds.operator

import unisson.model.kinds.KindExpr


/**
 *
 * Author: Ralf Mitschke
 * Date: 10.12.11
 * Time: 12:49
 *
 */
case class Not(kind: KindExpr)
        extends KindExpr
{
    lazy val asVespucciString = "!" + kind.asVespucciString
}