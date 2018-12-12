package sae.benchmark.company

import akka.remote.testkit.MultiNodeSpec
import idb.Relation
import idb.observer.NoOpObserver
import idb.query.QueryEnvironment
import sae.benchmark.BenchmarkMultiNodeSpec
import sae.benchmark.company.CompanyMultiNodeConfig._

class CompanyBenchmark4MultiJvmNode1 extends CompanyBenchmark4
class CompanyBenchmark4MultiJvmNode2 extends CompanyBenchmark4
class CompanyBenchmark4MultiJvmNode3 extends CompanyBenchmark4
class CompanyBenchmark4MultiJvmNode4 extends CompanyBenchmark4
class CompanyBenchmark4MultiJvmNode5 extends CompanyBenchmark4

object CompanyBenchmark4 {} // this object is necessary for multi-node testing

class CompanyBenchmark4 extends MultiNodeSpec(CompanyMultiNodeConfig)
	with BenchmarkMultiNodeSpec
	//Specifies the table setup
	with CompanyBenchmark
	//Specifies the number of measurements/warmups
	with DefaultPriorityConfig {

	override val benchmarkQuery = "query4"

	//Setup query environment
	implicit val env = QueryEnvironment.create(
		system,
		Map(
			publicHost -> (priorityPublic, permissionsPublic),
			productionHost -> (priorityProduction, permissionsProduction),
			purchasingHost -> (priorityPurchasing, permissionsPurchasing),
			employeesHost -> (priorityEmployees, permissionsEmployees),
			clientHost -> (priorityClient, permissionsClient)
		)
	)

	type ResultType = (Int, Int)

	object PublicDBNode extends PublicDBNode

	object ProductionDBNode extends ProductionDBNode {
		override protected def addComponentHook(componentId: Int): Unit = {
			if (componentId % 10 == 4)
				logLatency(componentId / 10, "query")
		}
	}

	object PurchasingDBNode extends PurchasingDBNode {
		override protected def addSupplierHook(supplierId: Int): Unit = {
			if (supplierId % 10 == 4) {
				logLatency(supplierId / 10, "query")
				logLatency((iterations + supplierId) / 10, "query")
			}
		}

		override protected def addSCHook(supplierId: Int, componentId: Int): Unit = {
			if (componentId % 10 == 4)
				logLatency(componentId / 10, "query")
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

			//			val q =
			//				SELECT ((c : Rep[Component], s : Rep[Supplier], sc : Rep[SC]) =>
			//					(c.id, s.id)
			//				) FROM (
			//					components, suppliers, scs
			//				) WHERE ((c, s, sc) =>
			//					sc.price < 60.00 AND sc.inventory >= 4 AND c.id == sc.componentId AND
			//						c.material == "Wood" AND s.id == sc.supplierId AND
			//						(s.city == "Darmstadt" OR s.city == "Hanau") AND
			//						s.name.startsWith("Bauhaus")
			//				)

			// Matches iteration == supplierId, (iteration || iterations + iteration) == componentId
			// if iteration % 10 == (3 || 4)
			val qSC =
			SELECT(*) FROM scs WHERE (sc =>
				sc.price < 60.00 AND sc.inventory >= 4)

			// Matches iteration == supplierId if iteration % 10 == (4 || 6) => 2  * iterations / 10 = #suppliers
			val qSupplier =
				SELECT(*
				) FROM suppliers WHERE (s =>
					(s.city == "Darmstadt" OR s.city == "Hanau") AND s.name.startsWith("Bauhaus"))

			// ==> 2 scs per supplier => 2 * iterations / 10 = #scs
			val qSupplierComponent =
				SELECT((s: Rep[Supplier], sc: Rep[SC]) =>
					(s.id, sc.componentId)
				) FROM(
					qSupplier, qSC
				) WHERE ((s, sc) =>
					s.id == sc.supplierId)

			// All components contain wood: 2 * iterations / 10 = #results
			val query4 =
				SELECT((c: Rep[Component], sc: Rep[(Int, Int)]) =>
					(c.id, sc._1)
				) FROM(
					components, qSupplierComponent
				) WHERE ((c: Rep[Component], sc: Rep[(Int, Int)]) =>
					c.id == sc._2 AND c.material == "Wood")

			//Define the root. The operators get distributed here.
			val r: idb.Relation[ResultType] =
				ROOT(clientHost, query4)

			// Setup latency recording
			r.addObserver(new NoOpObserver[ResultType] {
				override def added(v: (Int, Int)): Unit = {
					// Component / 10 id is used as latency id
					logLatency(v._1 / 10, "query", false)
				}

				override def addedAll(vs: Seq[(Int, Int)]): Unit = {
					vs.foreach(v => added(v))
				}
			})

			r
		}

		override protected def sleepUntilCold(expectedCount: Int, entryMode: Boolean): Unit = {
			try {
				super.sleepUntilCold(2 * iterations / 10)
			}
			catch {
				case e: IllegalArgumentException => log.error(e.getMessage + "\nIgnored for this test case, since it tends to produce a few duplicates")
			}
		}
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
