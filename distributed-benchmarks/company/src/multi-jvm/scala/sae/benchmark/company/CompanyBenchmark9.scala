package sae.benchmark.company

import akka.remote.testkit.MultiNodeSpec
import idb.Relation
import idb.observer.NoOpObserver
import idb.query.QueryEnvironment
import idb.schema.company.Supplier
import sae.benchmark.BenchmarkMultiNodeSpec
import sae.benchmark.company.CompanyMultiNodeConfig._

class CompanyBenchmark9MultiJvmNode1 extends CompanyBenchmark9
class CompanyBenchmark9MultiJvmNode2 extends CompanyBenchmark9
class CompanyBenchmark9MultiJvmNode3 extends CompanyBenchmark9
class CompanyBenchmark9MultiJvmNode4 extends CompanyBenchmark9
class CompanyBenchmark9MultiJvmNode5 extends CompanyBenchmark9

object CompanyBenchmark9 {} // this object is necessary for multi-node testing

class CompanyBenchmark9 extends MultiNodeSpec(CompanyMultiNodeConfig)
	with BenchmarkMultiNodeSpec
	//Specifies the table setup
	with CompanyBenchmark
	//Specifies the number of measurements/warmups
	with DefaultPriorityConfig {

	override val benchmarkQuery = "query9"

	//Setup query environment
	implicit val env: QueryEnvironment = QueryEnvironment.create(
		system,
		Map(
			publicHost -> (priorityPublic, permissionsPublic),
			productionHost -> (priorityProduction, permissionsProduction),
			purchasingHost -> (priorityPurchasing, permissionsPurchasing),
			employeesHost -> (priorityEmployees, permissionsEmployees),
			clientHost -> (priorityClient, permissionsClient)
		)
	)

	type ResultType = Supplier

	def getLatencyIdBySupplierId(supplierId: Int): Int =
		supplierId / 10 * 2 + supplierId % 10 + 4

	object PublicDBNode extends PublicDBNode

	object ProductionDBNode extends ProductionDBNode {
		override protected def addComponentHook(componentId: Int): Unit = {
			if (componentId < iterations && componentId % 10 >= 4 && componentId % 10 <= 5)
				logLatency(getLatencyIdBySupplierId(componentId), "query")
			else if (componentId >= iterations && (componentId - iterations) % 10 >= 4 && (componentId - iterations) % 10 <= 5)
				logLatency(getLatencyIdBySupplierId(componentId - iterations), "query")
		}
	}

	object PurchasingDBNode extends PurchasingDBNode {
		override protected def addSupplierHook(supplierId: Int): Unit = {
			if (supplierId % 10 >= 4 && supplierId % 10 <= 5)
				logLatency(getLatencyIdBySupplierId(supplierId), "query")
		}

		override protected def addSCHook(supplierId: Int, componentId: Int): Unit = {
			if (supplierId % 10 >= 4 && supplierId % 10 <= 5)
				logLatency(getLatencyIdBySupplierId(supplierId), "query")
		}
	}

	object EmployeesDBNode extends EmployeesDBNode

	object ClientNode extends ReceiveNode[ResultType]("client") {
		override def relation(): Relation[ResultType] = {
			//Write an i3ql query...
			import BaseCompany._
			import idb.schema.company._
			import idb.syntax.iql.IR._
			import idb.syntax.iql._


			val products: Rep[Query[Product]] = RECLASS(REMOTE GET(publicHost, "product-db"), labelPublic)
			val factories: Rep[Query[Factory]] = RECLASS(REMOTE GET(publicHost, "factory-db"), labelPublic)

			val components: Rep[Query[Component]] = RECLASS(REMOTE GET(productionHost, "component-db"), labelProduction)
			val pcs: Rep[Query[PC]] = RECLASS(REMOTE GET(productionHost, "pc-db"), labelProduction)
			val fps: Rep[Query[FP]] = RECLASS(REMOTE GET(productionHost, "fp-db"), labelProduction)

			val suppliers: Rep[Query[Supplier]] = RECLASS(REMOTE GET(purchasingHost, "supplier-db"), labelPurchasing)
			val scs: Rep[Query[SC]] = RECLASS(REMOTE GET(purchasingHost, "sc-db"), labelPurchasing)

			val employees: Rep[Query[Employee]] = RECLASS(REMOTE GET(employeesHost, "employee-db"), labelEmployees)
			val fes: Rep[Query[FE]] = RECLASS(REMOTE GET(employeesHost, "fe-db"), labelEmployees)


			val qa =
				SELECT((s: Rep[Supplier]) =>
					s.id
				) FROM suppliers WHERE (
					(s: Rep[Supplier]) =>
						s.city == "Darmstadt"
					)
			// All supplier ids from Darmstadt
			// 	- 4 <= id % 10 <= 5 (id = iteration)

			val qb =
				SELECT((sc: Rep[SC], c: Rep[Component]) =>
					sc.supplierId
				) FROM(scs, DECLASS(components, "lab:production")
				) WHERE (
					(sc: Rep[SC], c: Rep[Component]) =>
						sc.componentId == c.id AND c.material == "Wood"
					)
			// All supplier ids related to components including Wood, including duplicates
			// 	- Each supplier has two components with wood: component1Id = supplierId, component2Id = iterations + supplierId

			val qab = qa INTERSECT qb
			// All supplier ids from Darmstadt with components from Wood

			val query9 =
				SELECT((id: Rep[Int], s: Rep[Supplier]) =>
					s
				) FROM(qab, suppliers
				) WHERE (
					(id: Rep[Int], s: Rep[Supplier]) =>
						id == s.id
					)

			//Define the root. The operators get distributed here.
			val r: idb.Relation[ResultType] =
				ROOT(clientHost, query9)

			// Setup latency recording
			r.addObserver(new NoOpObserver[ResultType] {
				override def added(supplier: Supplier): Unit =
					logLatency(getLatencyIdBySupplierId(supplier.id), "query", false)

				override def addedAll(vs: Seq[ResultType]): Unit =
					vs foreach (v => added(v))
			})

			r
		}

		override protected def sleepUntilCold(expectedCount: Int, entryMode: Boolean): Unit =
			super.sleepUntilCold(iterations / 10 * 2)
	}

	"Hospital Benchmark" must {
		"run benchmark" in {
			runOn(rolePublic) { PublicDBNode.exec() }
			runOn(roleProduction) { ProductionDBNode.exec()	}
			runOn(rolePurchasing) { PurchasingDBNode.exec() }
			runOn(roleEmployees) { EmployeesDBNode.exec() }
			runOn(roleClient) { ClientNode.exec() }
		}
	}
}
