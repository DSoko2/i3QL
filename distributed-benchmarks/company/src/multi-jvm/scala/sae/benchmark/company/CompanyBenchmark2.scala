package sae.benchmark.company

import akka.remote.testkit.MultiNodeSpec
import idb.Relation
import idb.observer.NoOpObserver
import idb.query.QueryEnvironment
import sae.benchmark.BenchmarkMultiNodeSpec
import sae.benchmark.company.CompanyMultiNodeConfig._

class CompanyBenchmark2MultiJvmNode1 extends CompanyBenchmark2
class CompanyBenchmark2MultiJvmNode2 extends CompanyBenchmark2
class CompanyBenchmark2MultiJvmNode3 extends CompanyBenchmark2
class CompanyBenchmark2MultiJvmNode4 extends CompanyBenchmark2
class CompanyBenchmark2MultiJvmNode5 extends CompanyBenchmark2

object CompanyBenchmark2 {} // this object is necessary for multi-node testing

class CompanyBenchmark2 extends MultiNodeSpec(CompanyMultiNodeConfig)
	with BenchmarkMultiNodeSpec
	//Specifies the table setup
	with CompanyBenchmark
	//Specifies the number of measurements/warmups
	with DefaultPriorityConfig {

	override val benchmarkQuery = "query2"

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

	object PublicDBNode extends PublicDBNode {
		override protected def addProductHook(productId: Int): Unit = {
			logLatency(productId, "query")
		}
	}

	object ProductionDBNode extends ProductionDBNode {
		override protected def addPCHook(productId: Int, componentId: Int): Unit = {
			logLatency(productId, "query")
		}
	}

	object PurchasingDBNode extends PurchasingDBNode {
		override protected def addSupplierHook(supplierId: Int): Unit = {
			logLatency(supplierId, "query")
			logLatency(iterations + supplierId, "query")
		}

		override protected def addSCHook(supplierId: Int, componentId: Int): Unit = {
			logLatency(componentId, "query")
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

			// Get product to supplier relation
			//TODO: Here a cross product (instead of equi join) is generated!
			//			val query2: Rep[Query[ResultType]] =
			//				SELECT ( (s : Rep[Supplier], p : Rep[Product], pc : Rep[PC], sc : Rep[SC]) =>
			//					(pc.productId, sc.supplierId)
			//				) FROM (
			//					suppliers, products, pcs, scs
			//				) WHERE ( (s : Rep[Supplier], p : Rep[Product], pc : Rep[PC], sc : Rep[SC]) =>
			//					sc.componentId == pc.componentId AND pc.productId == p.id AND s.id == sc.supplierId
			//				)

			val qa =
				SELECT((s: Rep[Supplier], sc: Rep[SC]) =>
					(s.id, sc.componentId)
				) FROM(
					suppliers, scs
				) WHERE ((s: Rep[Supplier], sc: Rep[SC]) =>
					s.id == sc.supplierId
					)

			val qb =
				SELECT((p: Rep[Product], pc: Rep[PC]) =>
					(p.id, pc.componentId)
				) FROM(
					products, pcs
				) WHERE ((p: Rep[Product], pc: Rep[PC]) =>
					pc.productId == p.id
					)

			val query2 =
				SELECT DISTINCT ((a: Rep[(Int, Int)], b: Rep[(Int, Int)]) =>
					(a._1, b._1)
					) FROM(
					qa, qb
				) WHERE ((a: Rep[(Int, Int)], b: Rep[(Int, Int)]) =>
					a._2 == b._2
					)

			//Define the root. The operators get distributed here.
			val r: idb.Relation[ResultType] =
				ROOT(clientHost, query2)

			// Setup latency recording
			r.addObserver(new NoOpObserver[ResultType] {
				override def added(v: (Int, Int)): Unit = {
					// latency id is the product id
					logLatency(v._2, "query", false)
				}
			})

			r
		}

		override protected def sleepUntilCold(expectedCount: Int, entryMode: Boolean): Unit =
			super.sleepUntilCold(2 * iterations)
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
