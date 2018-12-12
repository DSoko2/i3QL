package sae.benchmark.company

import akka.remote.testkit.MultiNodeSpec
import idb.Relation
import idb.observer.NoOpObserver
import idb.query.QueryEnvironment
import sae.benchmark.BenchmarkMultiNodeSpec
import sae.benchmark.company.CompanyMultiNodeConfig._

class CompanyBenchmark7MultiJvmNode1 extends CompanyBenchmark7
class CompanyBenchmark7MultiJvmNode2 extends CompanyBenchmark7
class CompanyBenchmark7MultiJvmNode3 extends CompanyBenchmark7
class CompanyBenchmark7MultiJvmNode4 extends CompanyBenchmark7
class CompanyBenchmark7MultiJvmNode5 extends CompanyBenchmark7

object CompanyBenchmark7 {} // this object is necessary for multi-node testing

class CompanyBenchmark7 extends MultiNodeSpec(CompanyMultiNodeConfig)
	with BenchmarkMultiNodeSpec
	//Specifies the table setup
	with CompanyBenchmark
	//Specifies the number of measurements/warmups
	with DefaultPriorityConfig {

	override val benchmarkQuery = "query7"

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

	def getFactoryNumberById(factoryId: Int): Int =
		factoryId / 10 * 5 + Math.max(0, factoryId % 10 - 5)

	def getProductNumberById(productId: Int): Int =
		if (productId < iterations) {
			getFactoryNumberById(productId) * 2
		}
		else {
			getFactoryNumberById(productId - iterations) * 2 + 1
		}

	object PublicDBNode extends PublicDBNode {
		override protected def addProductHook(productId: Int): Unit = {
			if (productId < iterations && productId % 10 >= 5 || productId >= iterations && productId - iterations % 10 >= 5)
				logLatency(getProductNumberById(productId), "query")
		}
	}

	object ProductionDBNode extends ProductionDBNode {
		override protected def addFPHook(factoryId: Int, productId: Int): Unit = {
			if (factoryId % 10 >= 5)
				logLatency(getProductNumberById(productId), "query")
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


			val query7 =
				SELECT(
					(p: Rep[Product], fp: Rep[FP]) => p
				) FROM(
					products, fps
				) WHERE (
					(p: Rep[Product], fp: Rep[FP]) =>
						EXISTS(
							SELECT(
								(f: Rep[Factory]) => f
							) FROM factories
								WHERE (
								(f: Rep[Factory]) =>
									f.id == fp.factoryId AND f.city == "Darmstadt"
								)
						) AND
							p.id == fp.productId
					)
			// Selects all product that are produced in a factory in Darmstadt
			// 	- Factory is in Darmstadt if id % 10 >= 5 (id = iteration)
			//	- Each factory produces products with productId = factoryId and productId = iterations + factoryId

			//Define the root. The operators get distributed here.
			val r: idb.Relation[ResultType] =
				ROOT(clientHost, query7)

			// Setup latency rec
			r.addObserver(new NoOpObserver[ResultType] {
				override def added(product: ResultType): Unit = {
					// Latency id is product number
					logLatency(getProductNumberById(product.id), "query", false)
				}

				override def addedAll(vs: Seq[ResultType]): Unit =
					vs foreach (v => added(v))
			})

			r
		}

		override protected def sleepUntilCold(expectedCount: Int, entryMode: Boolean): Unit =
			super.sleepUntilCold(iterations)
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
