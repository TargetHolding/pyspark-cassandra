import scala.io

name := "pyspark-cassandra"

version := io.Source.fromFile("version.txt").mkString

organization := "TargetHolding"

scalaVersion := "2.10.5"

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0") 

libraryDependencies ++= Seq(
	"com.datastax.spark" %% "spark-cassandra-connector-java" % "1.4.1"
)

spName := "TargetHolding/pyspark-cassandra"

sparkVersion := "1.4.1"

sparkComponents += "streaming"

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

assemblyOption in assembly := (assemblyOption in assembly).value.copy(
	includeScala = false
)

assemblyMergeStrategy in assembly := {
	case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.last
	case x =>
		val oldStrategy = (assemblyMergeStrategy in assembly).value
		oldStrategy(x)
}

EclipseKeys.withSource := true
