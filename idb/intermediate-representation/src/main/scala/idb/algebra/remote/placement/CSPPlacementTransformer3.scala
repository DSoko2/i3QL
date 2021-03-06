package idb.algebra.remote.placement


import idb.algebra.QueryTransformerAdapter
import idb.algebra.base.RelationalAlgebraBase
import idb.algebra.exceptions.NoServerAvailableException
import idb.algebra.ir.{RelationalAlgebraIRAggregationOperators, RelationalAlgebraIRBasicOperators, RelationalAlgebraIRRemoteOperators, RelationalAlgebraIRSetTheoryOperators}
import idb.algebra.remote.taint.QueryTaint
import idb.lms.extensions.RemoteUtils
import idb.query.{Host, QueryEnvironment}

/**
  * CSPPlacementTransformer2 with new cost function for balanced load
  */
trait CSPPlacementTransformer3
	extends QueryTransformerAdapter with QueryTaint {
	//Defines whether the query tree should be use fragments bigger then single operators
	val USE_PRIVACY: Boolean = true
	// If true, total cost is network cost * load coast, otherwise it is independent sum with network priority
	val TOTAL_COST_PRODUCT: Boolean = true

	val IR: RelationalAlgebraBase
		with RelationalAlgebraIRBasicOperators
		with RelationalAlgebraIRRemoteOperators
		with RelationalAlgebraIRAggregationOperators
		with RelationalAlgebraIRSetTheoryOperators
		with RemoteUtils

	// Dont make it a case class, hash codes must be different for instances with identical property values!
	class Operator(
					  val query: IR.Rep[IR.Query[_]],
					  val load: Float,
					  val selectivity: Float,
					  val pinnedTo: Option[Int],
					  val children: Seq[Operator]
				  ) {
		lazy val outgoingLink: Float =
			if (children.isEmpty) selectivity * 1000
			else children.map(_.outgoingLink).sum * selectivity

		lazy val toList: Seq[Operator] =
			Seq(this) ++ children.flatMap(child => child.toList)

		override def toString: String = s"($hashCode: load $load, selectivity $selectivity, pinnedTo $pinnedTo, $query, children ${children.map(_.hashCode())}"
	}

	case class Link(
					   sender: Operator,
					   receiver: Operator,
					   load: Float
				   ) {
		override def toString: String = s"$sender => $receiver: $load"
	}

	override def transform[Domain: Manifest](relation: IR.Rep[IR.Query[Domain]])(implicit env: QueryEnvironment): IR.Rep[IR.Query[Domain]] = {

		println("global Defs = ")
		IR.globalDefsCache.toList.sortBy(t => t._1.id).foreach(println)


		val hostList = env.hosts.toSeq
		val hostId = hostList.zipWithIndex.toMap
		val hostCapacity = hostList.map(env.priorityOf)
		//Prepare data for CSP Solver function
		val operatorTree: Operator = operatorTreeFrom(relation, hostId)
		val operators: Seq[Operator] = operatorTree.toList

		val operatorHosts: Seq[Seq[Int]] = operators.map {
			operator =>
				if (USE_PRIVACY)
					env.findHostsFor(taintOf(operator.query).ids).map(h => hostId(h)).toSeq
				else
					hostId.values.toSeq
		}

		val links: Seq[Link] = operators.flatMap { operator =>
			operator.children.map { childOperator =>
				Link(childOperator, operator, childOperator.outgoingLink)
			}
		}


		//Compute placement using the CSP solver
		val placement: Seq[Int] = computePlacement(operators, operatorHosts, links, hostCapacity)

		if (placement == null)
			throw new NoServerAvailableException()


		//Translate results back to the AST
		implicit val placementMap: Map[IR.Rep[IR.Query[_]], Host] =
			placement.zipWithIndex.map { t =>
				val (hostId, operatorId) = t
				val operator = operators(operatorId)
				(operator.query, hostList(hostId))
			}.toMap

		println("placement Map = ")
		placementMap.toList.foreach(println)

		super.transform(addRemotes(relation))
	}

	private def operatorTreeFrom(query: IR.Rep[IR.Query[_]], hostId: Map[Host, Int]): Operator = {
		import IR._

		query match {
			//Base
			case QueryTable(_, _, _, h) =>
				new Operator(query, 0, 1f, Some(hostId(h)), Seq.empty)
			case QueryRelation(_, _, _, h) =>
				new Operator(query, 0, 1f, Some(hostId(h)), Seq.empty)
			case Def(Root(r, h)) =>
				new Operator(query, 0, 1f, Some(hostId(h)), Seq(operatorTreeFrom(r, hostId)))
			case Def(Materialize(r)) =>
				new Operator(query, 2, 1f, None, Seq(operatorTreeFrom(r, hostId)))

			//Basic Operators
			case Def(Selection(r, _)) =>
				new Operator(query, 1, 0.5f, None, Seq(operatorTreeFrom(r, hostId)))
			case Def(Projection(r, _)) =>
				new Operator(query, 1, 1f, None, Seq(operatorTreeFrom(r, hostId)))
			case Def(CrossProduct(r1, r2)) =>
				val c1 = operatorTreeFrom(r1, hostId)
				val c2 = operatorTreeFrom(r2, hostId)
				val selectivity = (c1.outgoingLink * c2.outgoingLink) / (c1.outgoingLink + c2.outgoingLink)
				new Operator(query, 8, selectivity, None, Seq(c1, c2))
			case Def(EquiJoin(r1, r2, _)) =>
				val c1 = operatorTreeFrom(r1, hostId)
				val c2 = operatorTreeFrom(r2, hostId)
				val selectivity = 2 * Math.min(c1.outgoingLink, c2.outgoingLink) / (c1.outgoingLink + c2.outgoingLink)
				new Operator(query, 4, selectivity, None, Seq(c1, c2))
			case Def(DuplicateElimination(r)) =>
				new Operator(query, 2, 0.5f, None, Seq(operatorTreeFrom(r, hostId)))
			case Def(Unnest(r, _)) =>
				new Operator(query, 1, 5f, None, Seq(operatorTreeFrom(r, hostId)))

			//Set theory operators
			case Def(UnionAdd(r1, r2)) =>
				val c1 = operatorTreeFrom(r1, hostId)
				val c2 = operatorTreeFrom(r2, hostId)
				new Operator(query, 1, 1f, None, Seq(c1, c2))
			case Def(UnionMax(r1, r2)) =>
				val c1 = operatorTreeFrom(r1, hostId)
				val c2 = operatorTreeFrom(r2, hostId)
				new Operator(query, 4, 1f, None, Seq(c1, c2))
			case Def(Intersection(r1, r2)) =>
				val c1 = operatorTreeFrom(r1, hostId)
				val c2 = operatorTreeFrom(r2, hostId)
				new Operator(query, 4, 0.5f, None, Seq(c1, c2))
			case Def(Difference(r1, r2)) =>
				val c1 = operatorTreeFrom(r1, hostId)
				val c2 = operatorTreeFrom(r2, hostId)
				new Operator(query, 4, 0.5f, None, Seq(c1, c2))

			//Aggregation operators
			case Def(AggregationSelfMaintained(r, _, _, _, _, _, _, _)) =>
				new Operator(query, 3, 2f, None, Seq(operatorTreeFrom(r, hostId)))
			case Def(AggregationNotSelfMaintained(r, _, _, _, _, _, _, _)) =>
				new Operator(query, 3, 2f, None, Seq(operatorTreeFrom(r, hostId)))

			//Remote
			case Def(Reclassification(r, _)) =>
				new Operator(query, 0, 1f, None, Seq(operatorTreeFrom(r, hostId)))
			case Def(Declassification(r, _)) =>
				new Operator(query, 0, 1f, None, Seq(operatorTreeFrom(r, hostId)))
			case Def(ActorDef(_, h, _)) =>
				new Operator(query, 0, 1f, Some(hostId(h)), Seq.empty)
		}
	}

	private def computePlacement(
									operators: Seq[Operator],
									operatorHosts: Seq[Seq[Int]],
									links: Seq[Link],
									hostCapacity: Seq[Int]
								): Seq[Int] = {
		import org.jacop.core._
		import org.jacop.constraints._
		import org.jacop.floats.core._
		import org.jacop.floats.constraints._
		import org.jacop.search._
		import org.jacop.constraints.binpacking.Binpacking

		println("Input: ")
		println("operators: " + operators)
		println("operator hosts: " + operatorHosts)
		println("links: " + links)
		println("capacities: " + hostCapacity)

		val startTime = System.nanoTime()


		val numOperators = operators.size
		val numLinks = links.size
		val numHosts = hostCapacity.size

		//Create global store
		val store = new Store()

		// Host on which the operator is placed on (operator -> host)
		val operatorHost = new Array[IntVar](numOperators)
		// Operator load (operator -> load)
		val operatorLoad = new Array[Int](numOperators)
		operators.zipWithIndex foreach { t =>
			val (operator, operatorId) = t
			operatorHost(operatorId) = new IntVar(store, s"operator${operatorId}host", 0, numHosts - 1)
			operatorLoad(operatorId) = operator.load.toInt
		}

		// Add host placement constraints
		operators.zipWithIndex.foreach { t =>
			val (operator, operatorId) = t

			// Pin operator on host if needed
			operator.pinnedTo match {
				case Some(hostId) => store.impose(new XeqC(operatorHost(operatorId), hostId))
				case None =>
			}

			// Add invalid operator on host placement constraints
			(0 until numHosts) foreach { hostId =>
				if (!operatorHosts(operatorId).contains(hostId))
					store.impose(new XneqC(operatorHost(operatorId), hostId))
			}
		}

		val operatorLoadSum = operatorLoad.sum
		val hostCapacitySum = hostCapacity.sum
		val maxCapacitySqr = Math.pow(hostCapacitySum, 2).toInt

		// Load on each host  (sum of operator loads on the host)
		val hostLoad = new Array[IntVar](numHosts)
		val hostLoadFloat = new Array[FloatVar](numHosts)
		// hostLoad * (capacity sum / (load sum * host capacity))
		val hostLoadQuotient = new Array[FloatVar](numHosts)
		// sqr(hostLoadQuotient)s
		val hostLoadCostFloat = new Array[FloatVar](numHosts)
		// Reduce variance in conversion (influence of range) by multiplication with 1000
		val hostLoadCost1000 = new Array[IntVar](numHosts)
		val hostLoadCostFloat1000 = new Array[FloatVar](numHosts)
		(0 until numHosts) foreach { host =>
			hostLoad(host) = new IntVar(store, s"host${host}load", 0, operatorLoadSum)
			hostLoadFloat(host) = new FloatVar(store, s"host${host}load", 0, operatorLoadSum)
			store.impose(new XeqP(hostLoad(host), hostLoadFloat(host)))

			hostLoadQuotient(host) = new FloatVar(store, s"host${host}loadQuotient", 0, hostCapacitySum)
			store.impose(new PmulCeqR(hostLoadFloat(host), hostCapacitySum.toDouble / (operatorLoadSum * hostCapacity(host)), hostLoadQuotient(host)))

			hostLoadCostFloat(host) = new FloatVar(store, s"host${host}loadCost" + host, 0, maxCapacitySqr)
			store.impose(new PmulQeqR(hostLoadQuotient(host), hostLoadQuotient(host), hostLoadCostFloat(host)))

			hostLoadCost1000(host) = new IntVar(store, s"host${host}loadCost1000" + host, 0, maxCapacitySqr * 1000)
			hostLoadCostFloat1000(host) = new FloatVar(store, s"host${host}loadCost" + host, 0, maxCapacitySqr * 1000)
			store.impose(new XeqP(hostLoadCost1000(host), hostLoadCostFloat1000(host)))
			store.impose(new PmulCeqR(hostLoadCostFloat(host), 1000, hostLoadCostFloat1000(host)))
		}
		//Define bin packing constraint (= load on all servers)
		// host load(i) = sum_j(operatorLoad(j) | operatorHost(j) == i)
		store.impose(new Binpacking(operatorHost, hostLoad, operatorLoad))


		val loadSum = new IntVar(store, "load-sum", 0, maxCapacitySqr * numHosts * 1000)
		val loadSumFloat = new FloatVar(store, "load-sum", 0, maxCapacitySqr * numHosts * 1000)
		store.impose(new XeqP(loadSum, loadSumFloat))
		store.impose(new SumInt(store, hostLoadCost1000, "==", loadSum))

		// Load on each link
		val linkLoad = new Array[IntVar](numLinks)
		val maxBandwidth = links.map(_.load.toInt).sum
		(0 until numLinks) foreach { linkId =>
			linkLoad(linkId) = new IntVar(store, "link" + linkId, 0, maxBandwidth)
		}
		links.zipWithIndex foreach { t =>
			val (link, linkId) = t
			store.impose(
				new IfThenElse(
					new XeqY(
						operatorHost(operators.indexOf(link.sender)),
						operatorHost(operators.indexOf(link.receiver))),
					new XeqC(linkLoad(linkId), 0),
					new XeqC(linkLoad(linkId), link.load.toInt)
				)
			)
		}

		val cost =
			if (TOTAL_COST_PRODUCT) {
				//Define network cost
				val networkCost = new IntVar(store, "network-sum", 0, maxBandwidth)
				store.impose(new SumInt(store, linkLoad, "==", networkCost))

				// cost = load * network
				val loadSum2 = new IntVar(store, "load-sum-small", 0, maxCapacitySqr * numHosts)
				val thousand = new IntVar(store, "1000", 1000, 1000)
				store.impose(new XeqC(thousand, 1000))
				store.impose(new XdivYeqZ(loadSum, thousand, loadSum2)) // Divide by thousand to avoid overflow
				val cost = new IntVar(store, "cost", 0, maxBandwidth * maxCapacitySqr * numHosts)
				store.impose(new XmulYeqZ(loadSum2, networkCost, cost))

				cost
			}
			else {
				//Define network cost
				val networkSum = new IntVar(store, "network-sum", 0, maxBandwidth)
				store.impose(new SumInt(store, linkLoad, "==", networkSum))
				val networkCost = new IntVar(store, "network-cost", 0, maxBandwidth * 1000)
				store.impose(new XmulCeqZ(networkSum, 1000, networkCost))

				// Load Cost
				val loadCost = new IntVar(store, "load-cost", 0, 1000)
				val loadCostFloat = new FloatVar(store, "load-cost", 0, 1000)
				store.impose(new XeqP(loadCost, loadCostFloat))
				store.impose(new PmulCeqR(loadSumFloat, 1000D / (maxCapacitySqr * 1000 * numHosts), loadCostFloat))


				// cost = load * network
				val cost = new IntVar(store, "cost", 0, maxBandwidth * 1000)
				store.impose(new XplusYeqZ(loadCost, networkCost, cost))

				cost
			}

		// Search for a solution (Minimize cost) and print results
		val search: Search[IntVar] = new DepthFirstSearch[IntVar]()
		val select: SelectChoicePoint[IntVar] =
			new InputOrderSelect[IntVar](store, operatorHost,
				new IndomainMin[IntVar]())
		val result: Boolean = search.labeling(store, select, cost)


		val endTime = System.nanoTime()

		Predef.println("Time: " + (endTime - startTime))
		Predef.println("Store >>>\n" + store + "\n<<< Store")

		if (result) {
			Predef.println("Solution:")
			for (op <- operatorHost)
				Predef.println(op)
			operatorHost.map(op => op.value())
		} else {
			Predef.println("*** No")
			null
		}

	}


	private def addRemotes[Domain: Manifest](
												query: IR.Rep[IR.Query[Domain]]
											)(
												implicit env: QueryEnvironment, placement: Map[IR.Rep[IR.Query[_]], Host]
											): IR.Rep[IR.Query[Domain]] = {

		val mDom = implicitly[Manifest[Domain]]

		import IR._

		def distributeUnary[TA: Manifest, T: Manifest](child: Rep[Query[TA]], build: Rep[Query[TA]] => Rep[Query[T]]): Rep[Query[T]] = {
			val host = placement(query)
			if (host != placement(child))
				build(remote(addRemotes(child), host))
			else
				build(addRemotes(child))
		}

		def distributeBinary[TA: Manifest, TB: Manifest, T: Manifest](c1: Rep[Query[TA]], c2: Rep[Query[TB]], build: (Rep[Query[TA]], Rep[Query[TB]]) => Rep[Query[T]]): Rep[Query[T]] = {
			val host = placement(query)
			val h1 = placement(c1)
			val h2 = placement(c2)

			if (host == h1 && host == h2)
				build(addRemotes(c1), addRemotes(c2))
			else if (host == h1 && host != h2)
				build(addRemotes(c1), remote(addRemotes(c2), host))
			else if (host != h1 && host == h2)
				build(remote(addRemotes(c1), host), addRemotes(c2))
			else
				build(remote(addRemotes(c1), host), remote(addRemotes(c2), host))
		}

		query match {
			//Base
			case QueryTable(_, _, _, _) => query
			case QueryRelation(_, _, _, _) => query
			case Def(Root(r, h)) => distributeUnary(r, (q: Rep[Query[Domain]]) => root(q, h))
			case Def(Materialize(r)) => distributeUnary(r, (q: Rep[Query[Domain]]) => materialize(q))

			//Basic Operators
			case Def(Selection(r, f)) => distributeUnary(r, (q: Rep[Query[Domain]]) => selection(q, f)(mDom, env))
			case Def(Projection(r, f)) => distributeUnary(r, (q: Rep[Query[Any]]) => projection(q, f))
			case Def(CrossProduct(r1, r2)) => distributeBinary(r1, r2, (q1: Rep[Query[Any]], q2: Rep[Query[Any]]) => crossProduct(q1, q2)).asInstanceOf[Rep[Query[Domain]]]
			case Def(EquiJoin(r1, r2, eqs)) => distributeBinary(r1, r2, (q1: Rep[Query[Any]], q2: Rep[Query[Any]]) => equiJoin(q1, q2, eqs)).asInstanceOf[Rep[Query[Domain]]]
			case Def(DuplicateElimination(r)) => distributeUnary(r, (q: Rep[Query[Domain]]) => duplicateElimination(q))
			case Def(Unnest(r, f)) => distributeUnary(r, (q: Rep[Query[Any]]) => unnest(q, f)).asInstanceOf[Rep[Query[Domain]]]

			//Set theory operators
			case Def(UnionAdd(r1, r2)) => distributeBinary(r1, r2, (q1: Rep[Query[Any]], q2: Rep[Query[Any]]) => unionAdd(q1, q2)).asInstanceOf[Rep[Query[Domain]]]
			case Def(UnionMax(r1, r2)) => distributeBinary(r1, r2, (q1: Rep[Query[Any]], q2: Rep[Query[Any]]) => unionMax(q1, q2)).asInstanceOf[Rep[Query[Domain]]]
			case Def(Intersection(r1, r2)) => distributeBinary(r1, r2, (q1: Rep[Query[Any]], q2: Rep[Query[Any]]) => intersection(q1, q2)).asInstanceOf[Rep[Query[Domain]]]
			case Def(Difference(r1, r2)) => distributeBinary(r1, r2, (q1: Rep[Query[Any]], q2: Rep[Query[Any]]) => difference(q1, q2)).asInstanceOf[Rep[Query[Domain]]]

			//Aggregation operators
			case Def(AggregationSelfMaintained(r, gr, start, fa, fr, fu, ck, conv)) =>
				distributeUnary(r, (q: Rep[Query[Any]]) => aggregationSelfMaintained(q, gr, start, fa, fr, fu, ck, conv))
			case Def(AggregationNotSelfMaintained(r, gr, start, fa, fr, fu, ck, conv)) =>
				distributeUnary(r, (q: Rep[Query[Any]]) => aggregationNotSelfMaintained(q, gr, start, fa, fr, fu, ck, conv))

			//Remote
			case Def(Reclassification(r, t)) => distributeUnary(r, (q: Rep[Query[Domain]]) => reclassification(q, t))
			case Def(Declassification(r, t)) => distributeUnary(r, (q: Rep[Query[Domain]]) => declassification(q, t))
			case Def(ActorDef(_, _, _)) => query

		}
	}

}
