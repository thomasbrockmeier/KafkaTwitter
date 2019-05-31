name := "KafkaTwitter"

version := "0.1"

scalaVersion := "2.12.8"

lazy val elasticsearchconsumer = project
  .settings(
    name := "ElasticsearchConsumer",
    libraryDependencies ++= commonDependencies ++ Seq(
      dependencies.elastic4sHttp,
    )
  )
lazy val twitterproducer = project
  .settings(
    name := "TwitterProducer",
    libraryDependencies ++= commonDependencies ++ Seq(
      dependencies.twitter
    )
  )

lazy val dependencies = new {
  val config          = "com.typesafe"              %   "config"              % "1.3.4"
  val twitter         = "org.twitter4j"             %   "twitter4j-stream"    % "4.0.7"
  val kafka           = "org.apache.kafka"          %   "kafka-clients"       % "2.2.0"
  val elastic4sHttp   = "com.sksamuel.elastic4s"    %%  "elastic4s-http"      % "6.5.1"
}

lazy val commonDependencies = Seq(
  dependencies.config,
  dependencies.kafka,
)
