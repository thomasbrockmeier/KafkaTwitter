package com.github.thomasbrockmeier.elasticsearchconsumer


import java.time.Duration
import java.util.{Collections, Properties}

import com.sksamuel.elastic4s.http.bulk.BulkResponse
import com.sksamuel.elastic4s.http.{ElasticClient, ElasticDsl, ElasticProperties, Response}
import com.typesafe.config.{Config, ConfigFactory}
import io.circe.Json
import io.circe.parser._
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.client.{BasicCredentialsProvider, DefaultConnectionKeepAliveStrategy}
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.elasticsearch.client.RestClientBuilder.{HttpClientConfigCallback, RequestConfigCallback}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

object ElasticsearchConsumer extends App {
  System.setProperty("java.net.preferIPv4Stack", "true")

  object Util {
    def getElasticsearch: ElasticClient = {
      val env: Config = ConfigFactory.load()

      val credentialsProvider = new BasicCredentialsProvider()
      credentialsProvider.setCredentials(
        AuthScope.ANY,
        new UsernamePasswordCredentials(
          env.getString("elasticsearch.access-key"),
          env.getString("elasticsearch.access-secret")
        )
      )

      ElasticClient(
        ElasticProperties(
          env.getString("elasticsearch.scheme")
            + env.getString("elasticsearch.host")
            + ":"
            + env.getString("elasticsearch.port")
        ),
        new RequestConfigCallback {
          override def customizeRequestConfig(requestConfigBuilder: RequestConfig.Builder): RequestConfig.Builder = {
            requestConfigBuilder
          }
        },
        new HttpClientConfigCallback {
          override def customizeHttpClient(httpClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder = {
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
            httpClientBuilder.setKeepAliveStrategy(DefaultConnectionKeepAliveStrategy.INSTANCE)
          }
        }
      )
    }

    val elasticSearch: ElasticClient = getElasticsearch
  }

  class ConsumerRunnable(topic: String, elasticType: String, bootstrapServers: String, groupId: String)
    extends Runnable with ElasticDsl {

    private val properties = new Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
//    properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100")  // Be nice to bonsai.io

    private val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](properties)
    consumer.subscribe(Collections.singletonList(topic))

    private def tweetId(json: String): String = {
      val parsed = parse(json).getOrElse(Json.Null)
      parsed.hcursor.get[String]("id_str") match {
        case Right(value) => value
        case Left(_) =>
          println("Error parsing tweet id!")
          ""
      }
    }


    def run(): Unit = {
      println("Starting event loop...")
      var amountOfTweets = 0
      try {
        while (true) {
          val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(100))
          if (!records.isEmpty) {
            val indexResponse: Future[Response[BulkResponse]] = Util.elasticSearch.execute {
              bulk (
                records.asScala.map( record => {
                  amountOfTweets += 1
                  indexInto(topic, elasticType).doc(record.value).id(tweetId(record.value))
                })
              )
            }
            indexResponse.onComplete {
              case Success(value) =>
                println("Indexed successfully: ", value)
                consumer.commitSync()
              case Failure(value) =>
                println("Failed to index: ", value)
                value.printStackTrace()
            }
            println(s"Total number of tweets indexed: $amountOfTweets")
          }

          Thread.sleep(1000)  // Let's not DOS bonsai.io
        }
      } catch {
        case _: WakeupException => println("Received shutdown signal!")
        case e: Throwable => println(e)
      } finally {
        consumer.close()
      }
    }

    def shutdown(): Unit = {
      println("Shutting down event loop...")
      consumer.wakeup()  // throws WakeUpException
    }
  }

  private val topic = s"tweets_about_${args(0)}"
  private val elasticType = "tweets"
  private val bootstrapServers = "127.0.0.1:9092"
  private val groupId = s"${args(0)}_to_elasticsearch_v2"

  private val consumerRunnable = new ConsumerRunnable(topic, elasticType, bootstrapServers, groupId)
  private val thread: Thread = new Thread(consumerRunnable)

  sys.addShutdownHook({
    println("ShutdownHook")
    consumerRunnable.shutdown()
    Util.elasticSearch.close()
    thread.interrupt()
  })

  thread.start()

}