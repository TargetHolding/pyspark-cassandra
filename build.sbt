name := "pyspark-cassandra"

version := "0.2.0"

organization := "TargetHolding"

scalaVersion := "2.10.5"
//scalaVersion := "2.11.7"
//crossScalaVersions := Seq("2.11.7", "2.10.4")

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0") 

libraryDependencies ++= Seq(
	"com.datastax.spark" %% "spark-cassandra-connector-java" % "1.4.0",
	"net.razorvine" % "pyrolite" % "4.10"
)

spName := "TargetHolding/pyspark-cassandra"

sparkVersion := "1.4.1"

sparkComponents += "streaming"

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

EclipseKeys.withSource := true
