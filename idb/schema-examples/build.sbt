/** Project */
name := "idb-schema-examples"

version := "0.0.1"

organization := "de.tud.cs.st.idb"


libraryDependencies ++= Seq(
    "EPFL" %% "lms" % "latest.integration"
)

parallelExecution in Test := false

logBuffered in Test := false