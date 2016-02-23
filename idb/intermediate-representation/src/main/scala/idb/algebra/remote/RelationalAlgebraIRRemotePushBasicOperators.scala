package idb.algebra.remote

import idb.algebra.ir.{RelationalAlgebraIRBasicOperators, RelationalAlgebraIRRemoteOperators}
import idb.query.QueryEnvironment

/**
 * @author Mirko Köhler
 */
trait RelationalAlgebraIRRemotePushBasicOperators
	extends RelationalAlgebraIRBasicOperators
	with RelationalAlgebraIRRemoteOperators{

	override def selection[Domain: Manifest] (
		relation: Rep[Query[Domain]],
		function: Rep[Domain => Boolean]
	)(implicit queryEnvironment : QueryEnvironment): Rep[Query[Domain]] = relation match {
		case Def(Remote(rel, thisDesc, thatDesc)) => remote(selection(rel, function), thisDesc, thatDesc)
		case _ => super.selection(relation, function)
	}

	override def projection[Domain: Manifest, Range: Manifest] (
		relation: Rep[Query[Domain]],
		function: Rep[Domain => Range]
	)(implicit queryEnvironment : QueryEnvironment): Rep[Query[Range]] = relation match {
		case Def(Remote(rel, thisDesc, thatDesc)) => remote(projection(rel, function), thisDesc, thatDesc)
		case _ => super.projection(relation, function)
	}



}
