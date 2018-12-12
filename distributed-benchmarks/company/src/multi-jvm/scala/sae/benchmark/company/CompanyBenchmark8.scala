package sae.benchmark.company

import akka.remote.testkit.MultiNodeSpec
import idb.Relation
import idb.observer.NoOpObserver
import idb.query.QueryEnvironment
import sae.benchmark.BenchmarkMultiNodeSpec
import sae.benchmark.company.CompanyMultiNodeConfig._

import scala.language.postfixOps

class CompanyBenchmark8MultiJvmNode1 extends CompanyBenchmark8
class CompanyBenchmark8MultiJvmNode2 extends CompanyBenchmark8
class CompanyBenchmark8MultiJvmNode3 extends CompanyBenchmark8
class CompanyBenchmark8MultiJvmNode4 extends CompanyBenchmark8
class CompanyBenchmark8MultiJvmNode5 extends CompanyBenchmark8

object CompanyBenchmark8 {} // this object is necessary for multi-node testing

class CompanyBenchmark8 extends MultiNodeSpec(CompanyMultiNodeConfig)
	with BenchmarkMultiNodeSpec
	//Specifies the table setup
	with CompanyBenchmark
	//Specifies the number of measurements/warmups
	with DefaultPriorityConfig {

	override val benchmarkQuery = "query8"

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

	def internalBarrier(name: String): Unit = {
		enterBarrier(name)
	}

	type ResultType = Int

	def getSupplierNumberById(supplierId: Int): Int =
		supplierId / 10 * 4 + supplierId % 10 - 4

	object PublicDBNode extends PublicDBNode

	object ProductionDBNode extends ProductionDBNode

	object PurchasingDBNode extends PurchasingDBNode {
		override protected def addSupplierHook(supplierId: ResultType): Unit = {
			if (supplierId % 10 >= 4 && supplierId % 10 <= 7)
				logLatency(getSupplierNumberById(supplierId), "query")
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
				SELECT((s: Rep[Supplier]) => s.id) FROM suppliers WHERE ((s: Rep[Supplier]) => s.city == "Darmstadt")

			val qb =
				SELECT((s: Rep[Supplier]) => s.id) FROM suppliers WHERE ((s: Rep[Supplier]) => s.city == "Hanau")

			val query8 = qa UNION ALL(qb)
			// Selects all suppliers from Darmstadt and Hanau
			// 	- All suppliers with 4 <= id % 10 <= 7 (id = iteration)

			//Define the root. The operators get distributed here.
			val r: idb.Relation[ResultType] =
				ROOT(clientHost, query8)

			r.addObserver(new NoOpObserver[ResultType] {
				override def added(supplierId: ResultType): Unit =
					logLatency(getSupplierNumberById(supplierId), "query", false)

				override def addedAll(vs: Seq[ResultType]): Unit =
					vs foreach (v => added(v))
			})

			r
		}

		override protected def sleepUntilCold(expectedCount: Int, entryMode: Boolean): Unit =
			super.sleepUntilCold(iterations / 10 * 4)
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
