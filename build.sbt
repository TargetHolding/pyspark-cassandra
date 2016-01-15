import scala.io

name := "pyspark-cassandra"

version := io.Source.fromFile("version.txt").mkString

organization := "TargetHolding"

scalaVersion := "2.10.5"

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0") 

libraryDependencies ++= Seq(
	"com.datastax.spark" %% "spark-cassandra-connector-java" % "1.5.0-RC1"
)

spName := "TargetHolding/pyspark-cassandra"

sparkVersion := "1.5.1"

sparkComponents += "streaming"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

assemblyOption in assembly := (assemblyOption in assembly).value.copy(
	includeScala = false
)

assemblyMergeStrategy in assembly <<= (assemblyMergeStrategy in assembly) {
  (old) => {
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case PathList("META-INF", xs @ _*) => MergeStrategy.last
	case x => MergeStrategy.last
  }
}

EclipseKeys.withSource := true
