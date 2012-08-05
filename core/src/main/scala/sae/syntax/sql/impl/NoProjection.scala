package sae.syntax.sql.impl

import sae.LazyView
import sae.syntax.sql.SELECT_CLAUSE_NO_PROJECTION

/**
 *
 * Author: Ralf Mitschke
 * Date: 03.08.12
 * Time: 20:00
 *
 */
case class NoProjection(distinct: Boolean = false)
    extends SELECT_CLAUSE_NO_PROJECTION
{
    def FROM[Domain <: AnyRef](relation: LazyView[Domain]) = FromNoProjection[Domain](relation, distinct)
}