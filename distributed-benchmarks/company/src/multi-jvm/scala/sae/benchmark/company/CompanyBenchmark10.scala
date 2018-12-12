package sae.benchmark.company

import akka.remote.testkit.MultiNodeSpec
import idb.Relation
import idb.observer.NoOpObserver
import idb.query.QueryEnvironment
import sae.benchmark.BenchmarkMultiNodeSpec
import sae.benchmark.company.CompanyMultiNodeConfig._

class CompanyBenchmark10MultiJvmNode1 extends CompanyBenchmark10
class CompanyBenchmark10MultiJvmNode2 extends CompanyBenchmark10
class CompanyBenchmark10MultiJvmNode3 extends CompanyBenchmark10
class CompanyBenchmark10MultiJvmNode4 extends CompanyBenchmark10
class CompanyBenchmark10MultiJvmNode5 extends CompanyBenchmark10

object CompanyBenchmark10 {} // this object is necessary for multi-node testing

class CompanyBenchmark10 extends MultiNodeSpec(CompanyMultiNodeConfig)
	with BenchmarkMultiNodeSpec
	//Specifies the table setup
	with CompanyBenchmark
	//Specifies the number of measurements/warmups
	with DefaultPriorityConfig {

	override val benchmarkQuery = "query10"

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

	type ResultType = idb.schema.company.Product

	object PublicDBNode extends PublicDBNode {
		override protected def addProductHook(productId: Int): Unit = {
			logLatency(productId, "query")
		}
	}

	object ProductionDBNode extends ProductionDBNode {
		override protected def addComponentHook(componentId: Int): Unit = {
			logLatency(componentId, "query")
			if (componentId >= iterations)
				logLatency(componentId - iterations, "query")
		}

		override protected def addPCHook(productId: Int, componentId: Int): Unit = {
			logLatency(productId, "query")
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


			val productsWithWood: Rep[Query[Product]] =
				SELECT DISTINCT ((c: Rep[Component], pc: Rep[PC], p: Rep[Product]) =>
					p
					) FROM(
					components, pcs, products
				) WHERE ((c, pc, p) =>
					c.material == "Wood" AND c.id == pc.componentId AND p.id == pc.productId
					)

			//Define the root. The operators get distributed here.
			val r: idb.Relation[ResultType] =
				ROOT(clientHost, productsWithWood).asMaterialized

			// Setup latency recording
			r.addObserver(new NoOpObserver[ResultType] {
				override def added(product: ResultType): Unit =
					logLatency(product.id, "query", false)

				override def addedAll(vs: Seq[ResultType]): Unit =
					vs foreach (v => added(v))
			})

			r
		}

		override protected def sleepUntilCold(expectedCount: Int, entryMode: Boolean): Unit =
			super.sleepUntilCold(iterations * 2)
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
