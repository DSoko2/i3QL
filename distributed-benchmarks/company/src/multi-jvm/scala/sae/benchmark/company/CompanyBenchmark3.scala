package sae.benchmark.company

import akka.remote.testkit.MultiNodeSpec
import idb.Relation
import idb.observer.NoOpObserver
import idb.query.QueryEnvironment
import sae.benchmark.BenchmarkMultiNodeSpec
import sae.benchmark.company.CompanyMultiNodeConfig._

class CompanyBenchmark3MultiJvmNode1 extends CompanyBenchmark3
class CompanyBenchmark3MultiJvmNode2 extends CompanyBenchmark3
class CompanyBenchmark3MultiJvmNode3 extends CompanyBenchmark3
class CompanyBenchmark3MultiJvmNode4 extends CompanyBenchmark3
class CompanyBenchmark3MultiJvmNode5 extends CompanyBenchmark3

object CompanyBenchmark3 {} // this object is necessary for multi-node testing

class CompanyBenchmark3 extends MultiNodeSpec(CompanyMultiNodeConfig)
	with BenchmarkMultiNodeSpec
	//Specifies the table setup
	with CompanyBenchmark
	//Specifies the number of measurements/warmups
	with DefaultPriorityConfig {

	override val benchmarkQuery = "query3"

	def latencyIdByEmployeeId(employeeId: Int): Int = {
		employeeId - employeeId / 10 - 1
	}

	def logLatencyByFactoryId(factoryId: Int, trace: String, logLatency: (Int, String, Boolean) => Unit): Unit = {
		(1 until 10).foreach(i =>
			logLatency(latencyIdByEmployeeId(10 * factoryId + i), trace, true))
	}

	object PublicDBNode extends PublicDBNode {
		override protected def addFactoryHook(factoryId: Int): Unit = {
			logLatencyByFactoryId(factoryId, "query", logLatency)
		}

		override protected def addProductHook(productId: Int): Unit = {
			val factoryId = if (productId < iterations) productId else  productId - iterations
			logLatencyByFactoryId(factoryId, "query", logLatency)
		}
	}

	object ProductionDBNode extends ProductionDBNode {
		override protected def addComponentHook(componentId: Int): Unit = {
			val factoryId = if (componentId < iterations) componentId else  componentId - iterations
			logLatencyByFactoryId(factoryId, "query", logLatency)
		}

		override protected def addFPHook(factoryId: Int, productId: Int): Unit = {
			logLatencyByFactoryId(factoryId, "query", logLatency)
		}

		override protected def addPCHook(productId: Int, componentId: Int): Unit = {
			val factoryId = if (productId < iterations) productId else  productId - iterations
			logLatencyByFactoryId(factoryId, "query", logLatency)
		}
	}

	object PurchasingDBNode extends PurchasingDBNode

	object EmployeesDBNode extends EmployeesDBNode {
		override protected def addEmployeeHook(employeeId: Int): Unit = {
			if (employeeId % 10 != 0) // Every 10th employee is not a worker
				logLatency(latencyIdByEmployeeId(employeeId), "query")
		}

		override protected def addFEHook(factoryId: Int, employeeId: Int): Unit = {
			if (employeeId % 10 != 0) // Every 10th employee is not a worker
				logLatency(latencyIdByEmployeeId(employeeId), "query")
		}
	}

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

	type ResultType = idb.schema.company.Employee

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
				SELECT((c: Rep[Component], pc: Rep[PC], p: Rep[Product]) =>
					p
				) FROM(
					components, pcs, products
				) WHERE ((c, pc, p) =>
					c.material == "Wood" AND c.id == pc.componentId AND p.id == pc.productId
					)


			val factoriesWithWood: Rep[Query[Factory]] =
				SELECT((p: Rep[Product], fp: Rep[FP], f: Rep[Factory]) =>
					f
				) FROM(
					productsWithWood, fps, factories
				) WHERE ((p, fp, f) =>
					p.id == fp.productId AND fp.factoryId == f.id
					)


			val workerWithWood =
				SELECT DISTINCT ((f: Rep[Factory], fe: Rep[FE], e: Rep[Employee]) =>
					e
					) FROM(
					DECLASS(factoriesWithWood, "lab:production"), fes, employees
				) WHERE ((f, fe, e) =>
					f.id == fe.factoryId AND e.id == fe.employeeId AND fe.job == "Worker"
					)

			//Define the root. The operators get distributed here.
			val r: idb.Relation[ResultType] =
				ROOT(clientHost, workerWithWood)

			// Setup latency recording
			r.addObserver(new NoOpObserver[ResultType] {
				override def added(employee: ResultType): Unit = {
					// latency id is the worker employees without id gaps starting with 0
					logLatency(latencyIdByEmployeeId(employee.id), "query", false)
				}

				override def addedAll(vs: Seq[ResultType]): Unit =
					vs.foreach(v => added(v))
			})

			r
		}

		override protected def sleepUntilCold(expectedCount: Int, entryMode: Boolean): Unit =
			super.sleepUntilCold(9 * iterations)
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
