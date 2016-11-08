package idb.algebra.compiler.boxing

import java.io.PrintStream

import idb.Relation
import idb.lms.extensions.ScalaCodegenExt
import idb.operators.impl.{AggregationForNotSelfMaintainableFunctions, AggregationForSelfMaintainableFunctions}

/**
  * Created by mirko on 18.10.16.
  */
case class BoxedAggregationNotSelfMaintained[Domain, Key, RangeA, RangeB, Range](
	relation: Relation[Domain],
	grouping: Domain => Key,
	start : RangeB,
	added : ((Domain, RangeB, Seq[Domain])) => RangeB,
	removed : ((Domain, RangeB, Seq[Domain])) => RangeB,
	updated: ((Domain, Domain, RangeB, Seq[Domain])) => RangeB,
	convertKey : Key => RangeA,
	convert : ((RangeA, RangeB)) => Range,
	isSet : Boolean
) extends Relation[Range] {

	@transient var aggregation : AggregationForNotSelfMaintainableFunctions[Domain, Key, RangeB, Range] = null

	def compile(compiler : ScalaCodegenExt): Unit = {

		aggregation = AggregationForNotSelfMaintainableFunctions.applyTupled[Domain, Key, RangeA, RangeB, Range] (
			relation,
			grouping = BoxedFunction.compile(grouping, compiler),
			start = start,
			added = BoxedFunction.compile(added, compiler),
			removed = BoxedFunction.compile(removed, compiler),
			updated = BoxedFunction.compile(updated, compiler),
			convertKey = BoxedFunction.compile(convertKey, compiler),
			convert = BoxedFunction.compile(convert, compiler),
			isSet = isSet
		)

		observers.foreach(o => aggregation.addObserver(o))
		observers.clear()
	}

	override def foreach[T](f: (Range) => T): Unit = aggregation.foreach(f)

	override def children: Seq[Relation[_]] = aggregation.children

	override protected def resetInternal(): Unit = aggregation.resetInternal()

	override protected[idb] def printInternal(out : PrintStream)(implicit prefix: String = " "): Unit =
		aggregation.printInternal(out)
}
