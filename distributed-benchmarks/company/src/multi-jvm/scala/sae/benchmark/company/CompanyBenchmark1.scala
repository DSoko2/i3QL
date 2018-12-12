package sae.benchmark.company

import akka.remote.testkit.MultiNodeSpec
import idb.Relation
import idb.observer.NoOpObserver
import idb.query.QueryEnvironment
import sae.benchmark.BenchmarkMultiNodeSpec
import sae.benchmark.company.CompanyMultiNodeConfig._

class CompanyBenchmark1MultiJvmNode1 extends CompanyBenchmark1
class CompanyBenchmark1MultiJvmNode2 extends CompanyBenchmark1
class CompanyBenchmark1MultiJvmNode3 extends CompanyBenchmark1
class CompanyBenchmark1MultiJvmNode4 extends CompanyBenchmark1
class CompanyBenchmark1MultiJvmNode5 extends CompanyBenchmark1

object CompanyBenchmark1 {} // this object is necessary for multi-node testing

class CompanyBenchmark1 extends MultiNodeSpec(CompanyMultiNodeConfig)
	with BenchmarkMultiNodeSpec
	//Specifies the table setup
	with CompanyBenchmark
	//Specifies the number of measurements/warmups
	with DefaultPriorityConfig {

	override val benchmarkQuery = "query1"

	override val latencyRecordingInterval: Int = 1 // Since anyways only 2 events occur, nothing else is reasonable

	type ResultType = (Int, String)

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

	object PublicDBNode extends PublicDBNode {
		override protected def addProductHook(productId: Int): Unit = {
			if (productId == iterations - 3) {
				logLatency(1, "query")
				logLatency(2, "query")
			}
		}
	}

	object ProductionDBNode extends ProductionDBNode {
		override protected def addComponentHook(componentId: Int): Unit = {
			if (componentId == iterations - 3)
				logLatency(1, "query")
			if (componentId == 2 * iterations - 3)
				logLatency(2, "query")
		}

		override protected def addPCHook(productId: Int, componentId: Int): Unit = {
			if (productId == iterations - 3)
				logLatency(if (componentId < iterations) 1 else 2, "query")
		}
	}

	object PurchasingDBNode extends PurchasingDBNode

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

			// Get components for the third last Billy product (Third last chosen, in order to avoid preliminary exit)
			val productName = s"Billy${iterations - 3}"
			val query1 =
				SELECT((p: Rep[Product], pc: Rep[PC], c: Rep[Component]) =>
					(c.id, c.name)
				) FROM(
					products, pcs, components
				) WHERE ((p: Rep[Product], pc: Rep[PC], c: Rep[Component]) =>
					p.name == productName AND pc.productId == p.id AND
						pc.componentId == c.id
					)

			//Define the root. The operators get distributed here.
			val r: idb.Relation[ResultType] =
				ROOT(clientHost, query1)

			// Setup latency recording
			r.addObserver(new NoOpObserver[ResultType] {
				override def added(v: ResultType): Unit = {
					logLatency(if (v._1 < iterations) 1 else 2, "query", false)
				}
			})

			r
		}

		override protected def sleepUntilCold(expectedCount: Int, entryMode: Boolean): Unit =
			super.sleepUntilCold(2)
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
