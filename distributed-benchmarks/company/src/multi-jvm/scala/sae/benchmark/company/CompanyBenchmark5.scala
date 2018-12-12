package sae.benchmark.company

import akka.remote.testkit.MultiNodeSpec
import idb.Relation
import idb.observer.NoOpObserver
import idb.query.QueryEnvironment
import idb.syntax.iql.impl._
import sae.benchmark.BenchmarkMultiNodeSpec
import sae.benchmark.company.CompanyMultiNodeConfig._
import sae.benchmark.recording.recorders.EventRecord

class CompanyBenchmark5MultiJvmNode1 extends CompanyBenchmark5
class CompanyBenchmark5MultiJvmNode2 extends CompanyBenchmark5
class CompanyBenchmark5MultiJvmNode3 extends CompanyBenchmark5
class CompanyBenchmark5MultiJvmNode4 extends CompanyBenchmark5
class CompanyBenchmark5MultiJvmNode5 extends CompanyBenchmark5

object CompanyBenchmark5 {} // this object is necessary for multi-node testing

class CompanyBenchmark5 extends MultiNodeSpec(CompanyMultiNodeConfig)
	with BenchmarkMultiNodeSpec
	//Specifies the table setup
	with CompanyBenchmark
	//Specifies the number of measurements/warmups
	with DefaultPriorityConfig {

	override val benchmarkQuery = "query5"

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

	type ResultType = (Int, Int, idb.schema.company.FE)

	object PublicDBNode extends PublicDBNode {
		override protected def addProductHook(productId: Int): Unit = {
			// Each product is produced in the factory with the same id
			// Record all billy products here
			if (productId < iterations)
				logLatency(productId * latencyRecordingInterval, "queryP")
		}

		eventRecorder.transferManipulation = record => {
			var records = Seq[EventRecord]()

			if (record.event.startsWith("latency.") && record.event.endsWith(".queryP.enter")) {
				val productId = record.event.split("\\.")(1).toInt / latencyRecordingInterval
				for (employeeId <- productId * 10 until productId * 10 + 10)
					if (employeeId % latencyRecordingInterval == 0)
						records = records :+ record.copy(event = s"latency.$employeeId.query.enter")
			}
			else
				records = Seq(record)

			records
		}
	}

	object ProductionDBNode extends ProductionDBNode {
		override protected def addFPHook(factoryId: Int, productId: Int): Unit = {
			// Record all factories products here
			if (productId < iterations)
				logLatency(factoryId * latencyRecordingInterval, "queryFP")
		}

		eventRecorder.transferManipulation = record => {
			var records = Seq[EventRecord]()

			if (record.event.startsWith("latency.") && record.event.endsWith("queryFP.enter")) {
				val factoryId = record.event.split("\\.")(1).toInt / latencyRecordingInterval
				for (employeeId <- factoryId * 10 until factoryId * 10 + 10)
					if (employeeId % latencyRecordingInterval == 0)
						records = records :+ record.copy(event = s"latency.$employeeId.query.enter")
			}
			else
				records = Seq(record)

			records
		}
	}

	object PurchasingDBNode extends PurchasingDBNode

	object EmployeesDBNode extends EmployeesDBNode {
		override protected def addFEHook(factoryId: Int, employeeId: Int): Unit = {
			logLatency(employeeId, "queryFE")
		}
	}

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

			//			val workersInFactory : Rep[Query[(Int, Int)]] =
			//				SELECT (
			//					(fid : Rep[Int]) => fid, COUNT(*)
			//				) FROM (fes) GROUP BY ((fe : Rep[FE]) => fe.factoryId)


			val a = COUNT.apply((fe: Rep[FE]) => fe)
			val aggregateFunction: AggregateFunctionSelfMaintained[FE, Int] = a.asInstanceOf[AggregateFunctionSelfMaintained[FE, Int]]

			val clause =
				GroupByClause1(
					(fe: Rep[FE]) => fe.factoryId,
					FromClause1(
						fes,
						SelectAggregateClause[Int, FE, (Int, Int, FE)](
							AggregateTupledFunctionSelfMaintained[Int, FE, Int, Int, (Int, Int, FE)](
								aggregateFunction.start,
								(x: Rep[(FE, Int)]) => aggregateFunction.added((x._1, x._2)),
								(x: Rep[(FE, Int)]) => aggregateFunction.removed((x._1, x._2)),
								(x: Rep[(FE, FE, Int)]) => aggregateFunction.updated((x._1, x._2, x._3)),
								(fid: Rep[Int]) => fid,
								fun((x: Rep[(Int, Int, FE)]) => (x._1, x._2, x._3))
							),
							false
						)
					)
				)

			val workersInFactory: Rep[Query[(Int, Int, FE)]] = plan(clause)

			val query5 = SELECT((fw: Rep[(Int, Int, FE)], fp: Rep[FP], p: Rep[Product]) =>
				fw
			) FROM(
				workersInFactory, fps, products
			) WHERE ((fw: Rep[(Int, Int, FE)], fp: Rep[FP], p: Rep[Product]) =>
				p.name.startsWith("Billy") AND fp.productId == p.id AND fp.factoryId == fw._1
				)
			// Sums up the number of workers in a factory producing Billy (every factory). Removes old entry if the
			// number changed and adds new one

			//Define the root. The operators get distributed here.
			val r: idb.Relation[ResultType] =
				ROOT(clientHost, query5)

			// Setup latency recording
			r.addObserver(new NoOpObserver[ResultType] {
				override def added(fw: (Int, Int, FE)): Unit = {
					// Employee id is used as latency id
					logLatency(fw._3.employeeId, "query", false)
				}

				override def addedAll(vs: Seq[(Int, Int, FE)]): Unit =
					vs foreach (v => added(v))
			})

			r
		}

		override protected def sleepUntilCold(expectedCount: Int, entryMode: Boolean): Unit =
		// iterations * 10 adds and iterations * 9 removes = event count, but sometimes removes and adds are aggregated
		// which leads to less events. Therefore => entryMode
			super.sleepUntilCold(iterations, true)
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
