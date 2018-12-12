package sae.benchmark.hospital

import idb.Table
import idb.metrics.ThroughputEvaluator
import idb.observer.Observer
import idb.query.RemoteHost
import idb.schema.hospital
import idb.schema.hospital._
import sae.benchmark.Benchmark
import sae.benchmark.hospital.HospitalMultiNodeConfig._

/**
  * Barriers that are used in the hospital benchmark:
  *
  * deployed - The tables have been deployed on their servers and the printer has been initialized.
  * compiled - The query has been compiled and deployed to the servers.
  *
  * sent-warmup - The warmup events have been sent (from the tables)
  *
  * resetted - The warmup events have been received and the data structures have been resetted.
  *
  * ready-measure - The classes needed for measurements have been initialized.
  * sent-measure - The measure events have been sent (from the tables).
  *
  * finished - The measurement has been finished.
  *
  *
  * deploy
  * compile
  * warmup-predata
  * warmup-data
  * warmup-finish
  * reset
  * measure-predata
  * measure-init
  * measure-data
  * measure-finish
  * finish
  */
trait HospitalBenchmark extends Benchmark with HospitalConfig {

	object BaseHospital extends HospitalSchema {
		override val IR = idb.syntax.iql.IR
	}

	object Data extends HospitalTestData


	type PersonType = Person
	type PatientType = Patient
	type KnowledgeType = KnowledgeData
	type ResultType = (Int, String, String)

	//Setup query environment
	val personHost = RemoteHost("personHost", node(node1))
	val patientHost = RemoteHost("patientHost", node(node2))
	val knowledgeHost = RemoteHost("knowledgeHost", node(node3))
	val clientHost = RemoteHost("clientHost", node(node4))

	object PersonDBNode extends DBNode("person", Seq("person-db"), 0, iterations) {

		override protected def iteration(dbs: Seq[Table[Any]], iteration: Int): Unit = {
			val db = dbs.head

			if (iteration % personSelectionInterval == 0) {
				// These records are relevant for the selection results of query1 - query4
				logLatency(iteration / personSelectionInterval, "query")
				db += hospital.Person(iteration, "John Doe", 1973)
			}
			else
				db += hospital.Person(iteration, "Jane Doe", 1960)
		}
	}

	object PatientDBNode extends DBNode("patient", Seq("patient-db"), 0, iterations) {

		import Data.Symptoms

		override protected def iteration(dbs: Seq[Table[Any]], iteration: Int): Unit = {
			val db = dbs.head

			// These records are relevant for the selection results of query1 - query4
			if (iteration % personSelectionInterval == 0)
				logLatency(iteration / personSelectionInterval, "query")
			db += hospital.Patient(iteration, 4, 2011, Seq(Symptoms.cough, Symptoms.chestPain))
		}
	}

	object KnowledgeDBNode extends DBNode("knowledge", Seq("knowledge-db"), 1, 0) {

		import Data.lungCancer1

		override protected def initIteration(dbs: Seq[Table[Any]], iteration: Int): Unit = {
			val db = dbs.head
			db += lungCancer1
		}
	}

	trait HospitalReceiveNode extends ReceiveNode[ResultType] {

		override def measurementRecordingInit(recorderRelations: Map[String, ThroughputEvaluator[_]] = null): Unit = {
			super.measurementRecordingInit(recorderRelations)
			setupLatencyRecording()
		}

		private def setupLatencyRecording(): Unit = {
			r.addObserver(new Observer[ResultType] {
				override def added(v: (Int, String, String)): Unit = {
					logLatency(v._1 / personSelectionInterval, "query", false)
				}

				override def addedAll(vs: Seq[(Int, String, String)]): Unit = {}

				override def updated(oldV: (Int, String, String), newV: (Int, String, String)): Unit = {}

				override def removed(v: (Int, String, String)): Unit = {}

				override def removedAll(vs: Seq[(Int, String, String)]): Unit = {}
			})
		}

		override protected def sleepUntilCold(expectedCount: Int, entryMode: Boolean): Unit =
			super.sleepUntilCold(iterations / personSelectionInterval)

	}

}
