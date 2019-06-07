name := "KafkaTwitter"

version := "0.1"

scalaVersion := "2.12.8"


lazy val common = project
  .settings(
    name := "common",
    libraryDependencies ++= commonDependencies ++ Seq(
      dependencies.twitter
    ),
    resolvers += "confluent" at "https://packages.confluent.io/maven/",
  )

lazy val elasticsearchconsumer = project
  .settings(
    name := "ElasticsearchConsumer",
    libraryDependencies ++= commonDependencies ++ Seq(
      dependencies.elastic4sHttp,
      dependencies.circe,
    ),
  )
  .dependsOn(common)
  .aggregate(common)

lazy val twitterproducer = project
  .settings(
    name := "TwitterProducer",
    libraryDependencies ++= commonDependencies ++ Seq(
      dependencies.twitter
    )
  )
  .dependsOn(common)
  .aggregate(common)

lazy val dependencies = new {
  val config              = "com.typesafe"            %   "config"                % "1.3.4"
  val twitter             = "org.twitter4j"           %   "twitter4j-stream"      % "4.0.7"
  val kafka               = "org.apache.kafka"        %   "kafka-clients"         % "2.2.0"
  val elastic4sHttp       = "com.sksamuel.elastic4s"  %%  "elastic4s-http"        % "6.5.1"
  val circe               = "io.circe"                %%  "circe-parser"          % "0.11.1"
  val avro                = "org.apache.avro"         %   "avro"                  % "1.9.0"
  val kafkaAvroSerializer = "io.confluent"            %   "kafka-avro-serializer" % "3.3.1"
  val jackson             = "org.codehaus.jackson"    %   "jackson-mapper-asl"    % "1.9.13"
}

lazy val commonDependencies = Seq(
  dependencies.jackson,
  dependencies.avro,
  dependencies.config,
  dependencies.kafka,
  dependencies.kafkaAvroSerializer,
)
