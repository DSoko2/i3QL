package sae.benchmark.company

import akka.remote.testkit.MultiNodeSpec
import idb.Relation
import idb.observer.NoOpObserver
import idb.query.QueryEnvironment
import sae.benchmark.BenchmarkMultiNodeSpec
import sae.benchmark.recording.recorders.EventRecord

class CompanyBenchmark6MultiJvmNode1 extends CompanyBenchmark6
class CompanyBenchmark6MultiJvmNode2 extends CompanyBenchmark6
class CompanyBenchmark6MultiJvmNode3 extends CompanyBenchmark6
class CompanyBenchmark6MultiJvmNode4 extends CompanyBenchmark6
class CompanyBenchmark6MultiJvmNode5 extends CompanyBenchmark6

object CompanyBenchmark6 {} // this object is necessary for multi-node testing

class CompanyBenchmark6 extends MultiNodeSpec(CompanyMultiNodeConfig)
	with BenchmarkMultiNodeSpec
	//Specifies the table setup
	with CompanyBenchmark
	//Specifies the number of measurements/warmups
	with DefaultPriorityConfig {

	override val benchmarkQuery = "query6"

	import CompanyMultiNodeConfig._

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

	// Factors for latency recording
	val factoryCount: Int = iterations / 2
	val productCount: Int = iterations / 10

	def getFactoryNumberById(factoryId: Int): Int =
		factoryId / 10 * 5 + Math.max(0, factoryId % 10 - 5)

	def getProductNumberById(productId: Int): Int =
		(productId - iterations) / 10

	def getLatencyId(factoryNumber: Int, productNumber: Int): Int =
		factoryNumber * productCount + productNumber

	object PublicDBNode extends PublicDBNode {
		override protected def addFactoryHook(factoryId: Int): Unit = {
			// Log every relevant factory, because it is quite sure it will be in one of the relevant latency traces
			if (factoryId % 10 >= 5)
				logLatency(getFactoryNumberById(factoryId) * latencyRecordingInterval, "queryF")
		}

		override protected def addProductHook(productId: Int): Unit = {
			// Log every relevant product, because it is quite sure it will be in one of the relevant latency traces
			if (productId >= iterations && productId % 10 == 0)
				logLatency(getProductNumberById(productId) * latencyRecordingInterval, "queryP")
		}

		// Multiply latency records in post processing in order to keep overhead during measurement low
		eventRecorder.transferManipulation = record => {
			var records = Seq[EventRecord]()

			if (record.event.startsWith("latency.") && record.event.endsWith(".queryF.enter")) {
				val factoryNumber = record.event.split("\\.")(1).toInt / latencyRecordingInterval
				for (productNumber <- 0 until productCount) {
					val recordId = getLatencyId(factoryNumber, productNumber)
					if (recordId % latencyRecordingInterval == 0)
						records = records :+ record.copy(event = s"latency.$recordId.query.enter")
				}
			}
			else if (record.event.startsWith("latency.") && record.event.endsWith(".queryP.enter")) {
				val productNumber = record.event.split("\\.")(1).toInt / latencyRecordingInterval
				for (factoryNumber <- 0 until factoryCount) {
					val recordId = getLatencyId(factoryNumber, productNumber)
					if (recordId % latencyRecordingInterval == 0)
						records = records :+ record.copy(event = s"latency.$recordId.query.enter")
				}
			}
			else
				records = Seq(record)

			records
		}

	}

	object ProductionDBNode extends ProductionDBNode

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


			val query6 = SELECT((f: Rep[Factory], p: Rep[Product]) =>
				(f.id, p.id)
			) FROM(
				factories, products
			) WHERE ((f, p) =>
				f.city == "Darmstadt" AND p.name.startsWith("Knut") AND p.name.endsWith("0")
				)
			// Equi joins:
			//		- Factories, where id % 10 >= 5 (id = iteration)
			//		- Products, where id >= iterations and id % 10 = 0 (id = iteration)

			//Define the root. The operators get distributed here.
			val r: idb.Relation[ResultType] =
				ROOT(clientHost, query6)

			r.addObserver(new NoOpObserver[ResultType] {
				override def added(v: (Int, Int)): Unit = {
					logLatency(getLatencyId(getFactoryNumberById(v._1), getProductNumberById(v._2)), "query", false)
				}

				override def addedAll(vs: Seq[(Int, Int)]): Unit = {
					vs.foreach(v => added(v))
				}
			})

			r
		}

		override protected def sleepUntilCold(expectedCount: Int, entryMode: Boolean): Unit = {
			super.sleepUntilCold(factoryCount * productCount)
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
