# Only required if working with the DataStax Spark Cassandra Connector
set -e

group_id="com.datastax.cassandra"
artifact_id="spark-cassandra-connector-java"
version="1.2.0"

echo "Installing DataStax Spark Cassandra Connector JAR to local maven repository."
echo "groupId: $group_id"
echo "artifact_id: $artifact_id"
echo "version: $version"

mvn install:install-file -DcreateChecksum=true -Dfile=maven_repo/spark-cassandra-connector-java-assembly-1.2.0-SNAPSHOT.jar -DgroupId=$group_id -DartifactId=$artifact_id -Dversion=$version -Dpackaging=jar -DlocalRepositoryPath=maven_repo
mvn clean package
