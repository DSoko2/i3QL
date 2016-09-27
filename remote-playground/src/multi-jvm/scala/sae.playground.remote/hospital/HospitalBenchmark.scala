package sae.playground.remote.hospital

import java.io.FileOutputStream

import akka.actor.{ActorPath, Address, Props}
import akka.remote.testkit.MultiNodeSpec
import akka.testkit.ImplicitSender
import idb.BagTable
import idb.algebra.ir.{RelationalAlgebraIRBasicOperators, _}
import idb.algebra.print.RelationalAlgebraPrintPlan
import idb.lms.extensions.FunctionUtils
import idb.lms.extensions.operations.{OptionOpsExp, SeqOpsExpExt, StringOpsExpExt}
import idb.operators.impl.{ProjectionView, SelectionView}
import idb.query.{QueryEnvironment, RemoteHost}
import idb.remote._
import idb.query._
import idb.query.colors._
import idb.syntax.iql.compilation.RemoteActor
import sae.example.hospital.data._
import sae.playground.remote.STMultiNodeSpec

import scala.virtualization.lms.common.{ScalaOpsPkgExp, StaticDataExp, StructExp, TupledFunctionsExp}

class HospitalBenchmarkMultiJvmNode1 extends HospitalBenchmark
class HospitalBenchmarkMultiJvmNode2 extends HospitalBenchmark
class HospitalBenchmarkMultiJvmNode3 extends HospitalBenchmark
class HospitalBenchmarkMultiJvmNode4 extends HospitalBenchmark

object HospitalBenchmark {} // this object is necessary for multi-node testing

class HospitalBenchmark extends MultiNodeSpec(HospitalConfig)
	with STMultiNodeSpec with ImplicitSender {

	import HospitalConfig._
	import HospitalBenchmark._

	def initialParticipants = roles.size

	//Setup query environment
	val personHost = RemoteHost("personHost", node(node1))
	val patientHost = RemoteHost("patientHost", node(node2))
	val knowledgeHost = RemoteHost("knowledgeHost", node(node3))
	val clientHost = RemoteHost("clientHost", node(node4))

	object BaseHospital extends HospitalSchema {
		override val IR = idb.syntax.iql.IR
	}
	import BaseHospital._

	object Data extends HospitalTestData
	import Data._

	implicit val env = QueryEnvironment.create(
		system,
		Map(
			personHost -> Set("red"),
			patientHost -> Set("red", "green", "purple"),
			knowledgeHost -> Set("purple"),
			clientHost -> Set("white") //For now: Client has its own permission to simulate pushing queries down
		)
	)

	"A hospital" must {
		"work for three servers (without client)" in {
			/*
				Person Server
			 */
			runOn(node1) {
				import idb.syntax.iql._

				val db = BagTable.empty[(Long, Person)]
				REMOTE RELATION (db, "person-db")

				enterBarrier("deployed")
				//The query gets compiled here...
				enterBarrier("compiled")

				(1 to 1000000).foreach(i => {
					db += ((System.currentTimeMillis(), sae.example.hospital.data.Person(i, "John Doe", 1973)))
				})

				enterBarrier("sent")
			}

			/*
				Patient Server
			 */
			runOn(node2) {
				import idb.syntax.iql._

				val db = BagTable.empty[(Long, Patient)]
				REMOTE RELATION (db, "patient-db")

				enterBarrier("deployed")
				//The query gets compiled here...
				enterBarrier("compiled")

				(1 to 1000000).foreach(i => {
					db += ((System.currentTimeMillis(),  sae.example.hospital.data.Patient(i, 4, 2011, Seq(Symptoms.cough, Symptoms.chestPain))))
				})

				enterBarrier("sent")


			}

			/*
				Knowledge Server
			 */
			runOn(node3) {
				import idb.syntax.iql._

				val db = BagTable.empty[(Long, KnowledgeData)]
				REMOTE RELATION (db, "knowledge-db")

				enterBarrier("deployed")
				//The query gets compiled here...
				enterBarrier("compiled")

				db += ((System.currentTimeMillis(), lungCancer1))

				enterBarrier("sent")
			}

			/*
				Client
			 */
			runOn(node4) {
				enterBarrier("deployed")

				import idb.syntax.iql._
				import idb.syntax.iql.IR._

				//Create variables for all the remote tables
				val personDB : Rep[Query[(Long, Person)]] =
					REMOTE FROM (personHost, "person-db", Color("red"))
				val patientDB : Rep[Query[(Long, Patient)]] =
					REMOTE FROM (patientHost, "patient-db", Color("green"))
				val knowledgeDB : Rep[Query[(Long, KnowledgeData)]] =
					REMOTE FROM (knowledgeHost, "knowledge-db", Color("purple"))

				//Write an i3ql query...
				val q1 =
					SELECT DISTINCT (
						(person: Rep[(Long, Person)], patientSymptom: Rep[((Long, Patient), String)], knowledgeData: Rep[(Long, KnowledgeData)]) => (person._1, patientSymptom._1._1, knowledgeData._1, person._2.personId, knowledgeData._2.diagnosis)
					) FROM (
						personDB, UNNEST(patientDB, (x: Rep[(Long, Patient)]) => x._2.symptoms), knowledgeDB
					) WHERE	(
						(person: Rep[(Long, Person)], patientSymptom: Rep[((Long, Patient), String)], knowledgeData: Rep[(Long, KnowledgeData)]) =>
								person._2.personId == patientSymptom._1._2.personId AND
								patientSymptom._2 == knowledgeData._2.symptom AND
								knowledgeData._2.symptom == Symptoms.cough
					)

				//... and add ROOT. Workaround: Reclass the data to make it pushable to the client node.
				val q = ROOT(RECLASS(q1, Color("white")), clientHost)


				//Print the LMS tree representation
				val printer = new RelationalAlgebraPrintPlan {
					override val IR = idb.syntax.iql.IR
				}
				Predef.println(printer.quoteRelation(q))

				//Compile the LMS tree and then materialize for further testing purposes
				val r : Relation[(Long, Long, Long, Int, String)] = q
				RemoteActor.forward(system, r) //TODO: Add this to ROOT
				//Print the runtime class representation
				Predef.println(r.prettyprint(" "))

				//Add observer for testing purposes
				import idb.evaluator.BenchmarkEvaluator
				val benchmark = new BenchmarkEvaluator[(Long, Long, Long, Int, String)](r, t => scala.math.max(t._1, scala.math.max(t._2, t._3)) )


				enterBarrier("compiled")
				//The tables are now sending data
				enterBarrier("sent")
				val out: java.io.PrintStream = new java.io.PrintStream(new FileOutputStream("hospital-benchmark.txt", true))
				out.println(benchmark.getSummary(System.currentTimeMillis()))
				out.close()
			}

		}
	}
}

