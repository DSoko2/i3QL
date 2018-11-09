package sae.benchmark.company

import akka.remote.testkit.MultiNodeSpec
import idb.{Relation, algebra}
import idb.algebra.print.RelationalAlgebraPrintPlan
import idb.query.taint._
import idb.query.{QueryEnvironment, RemoteHost}
import idb.syntax.iql.IR
import sae.benchmark.BenchmarkMultiNodeSpec

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

	override val benchmarkQuery = "3"


	import CompanyMultiNodeConfig._

	//Setup query environment
	val publicHost = RemoteHost("public-host", node(rolePublic))
	val productionHost = RemoteHost("production-host", node(roleProduction))
	val purchasingHost = RemoteHost("purchasing-host", node(rolePurchasing))
	val employeesHost = RemoteHost("employees-host", node(roleEmployees))
	val clientHost = RemoteHost("clients-host", node(roleClient))

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

	def internalBarrier(name : String): Unit = {
		enterBarrier(name)
	}

	type ResultType = idb.schema.company.Employee

	object ClientNode extends ReceiveNode[ResultType]("client") {
		override def relation(): Relation[ResultType] = {
			//Write an i3ql query...
			import BaseCompany._
			import idb.syntax.iql.IR._
			import idb.syntax.iql._
			import idb.schema.company._


			val products : Rep[Query[Product]] = RECLASS (REMOTE GET (publicHost, "product-db"), labelPublic)
			val factories : Rep[Query[Factory]] = RECLASS (REMOTE GET (publicHost, "factory-db"), labelPublic)

			val components : Rep[Query[Component]] = RECLASS (REMOTE GET (productionHost, "component-db"), labelProduction)
			val pcs : Rep[Query[PC]] = RECLASS (REMOTE GET (productionHost, "pc-db"), labelProduction)
			val fps : Rep[Query[FP]] = RECLASS (REMOTE GET (productionHost, "fp-db"), labelProduction)

			val suppliers : Rep[Query[Supplier]] = RECLASS (REMOTE GET (purchasingHost, "supplier-db"), labelPurchasing)
			val scs : Rep[Query[SC]] = RECLASS (REMOTE GET (purchasingHost, "sc-db"), labelPurchasing)

			val employees : Rep[Query[Employee]] = RECLASS (REMOTE GET (employeesHost, "employee-db"), labelEmployees)
			val fes : Rep[Query[FE]] = RECLASS (REMOTE GET (employeesHost, "fe-db"), labelEmployees)

			val productsWithWood : Rep[Query[Product]] =
				SELECT ((c : Rep[Component], pc : Rep[PC], p : Rep[Product]) =>
					p
				) FROM (
					components, pcs, products
				) WHERE ( (c, pc, p) =>
					c.material == "Wood" AND c.id == pc.componentId AND p.id == pc.productId
				)


			val factoriesWithWood : Rep[Query[Factory]] =
				SELECT ((p : Rep[Product], fp : Rep[FP], f : Rep[Factory]) =>
					f
				) FROM (
					productsWithWood, fps, factories
				) WHERE ( (p, fp, f) =>
					p.id == fp.productId AND fp.factoryId == f.id
				)


			val workerWithWood =
				SELECT DISTINCT ((f : Rep[Factory], fe : Rep[FE], e : Rep[Employee]) =>
					e
				) FROM (
					DECLASS (factoriesWithWood, "lab:production"), fes, employees
				) WHERE ((f, fe, e) =>
					f.id == fe.factoryId AND e.id == fe.employeeId AND fe.job == "Worker"
				)


			//Compile to LMS representation (only needed for printing)
			val query : Rep[Query[ResultType]] = workerWithWood

			//Define the root. The operators get distributed here.
			val r : idb.Relation[ResultType] =
			ROOT(clientHost, query)
			r
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