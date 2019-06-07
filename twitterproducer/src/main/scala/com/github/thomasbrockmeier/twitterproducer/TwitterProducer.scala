package com.github.thomasbrockmeier.twitterproducer

import java.util.Properties

import com.github.thomasbrockmeier.common.Serde
import com.typesafe.config.{Config, ConfigFactory}
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroSerializer}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import twitter4j._
import twitter4j.conf.{Configuration, ConfigurationBuilder}

object TwitterProducer extends App {
  object Util {
    def getTwitterStream: TwitterStream = {
      val config: Configuration = new ConfigurationBuilder()
        .setDebugEnabled(true)
        .setJSONStoreEnabled(true)
        .setOAuthConsumerKey(env.getString("twitter.api-key"))
        .setOAuthConsumerSecret(env.getString("twitter.api-secret-key"))
        .setOAuthAccessToken(env.getString("twitter.access-token"))
        .setOAuthAccessTokenSecret(env.getString("twitter.access-token-secret"))
        .build

      new TwitterStreamFactory(config).getInstance()
    }

    def getKafkaProducer: KafkaProducer[String, GenericRecord] = {
      val bootstrapServers: String = "http://127.0.0.1:9092"

      val properties = new Properties()
      properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
      properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
      properties.setProperty(
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        env.getString("schema-registry.url")
      )

      // Explicit settings for idempotent producer
      properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
      properties.setProperty(ProducerConfig.ACKS_CONFIG, "all")
      properties.setProperty(ProducerConfig.RETRIES_CONFIG, Int.MaxValue.toString)
      properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")

      // High throughput producer
      properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
      properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20")
      properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, (32 * 1024).toString)  // 32 KB

      new KafkaProducer[String, GenericRecord](properties)
    }

    def simpleStatusListener(topic: String): StatusListener = new StatusListener() {
      val serde: Serde = new Serde

      def onStatus(status: Status) {
        println("[INFO]  Message received, pushing to Kafka")
        println(s"[DEBUG] ${status.getText}")
        try {
          val tweet = serde.serializeStatus(status)
          kafkaProducer.send(
            new ProducerRecord[String, GenericRecord](
              s"tweets_about_$topic",
              null,
              tweet
            ),
            (_: RecordMetadata, exception: Exception) => {
              exception match {
                case e: Throwable =>
                  println(s"[ERROR] $e")
                  e.printStackTrace()
                case _ => println(s"[DEBUG] Success")
              }
            }
          )
        } catch {
          case e => e.printStackTrace()
        }
      }
      def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}
      def onTrackLimitationNotice(numberOfLimitedStatuses: Int) {}
      def onException(ex: Exception) {}
      def onScrubGeo(arg0: Long, arg1: Long) {}
      def onStallWarning(warning: StallWarning) {}
    }

    private val env: Config = ConfigFactory.load()

    val twitterStream: TwitterStream = getTwitterStream
    val kafkaProducer: KafkaProducer[String, GenericRecord] = getKafkaProducer
  }

  class TwitterStreamRunnable(subject: String = "scala") extends Runnable {
    def run(): Unit = {
      println("[DEBUG] Starting new listener...")

      val subject = args(0)
      Util.twitterStream.addListener(Util.simpleStatusListener(subject))
      Util.twitterStream.filter(new FilterQuery().track(subject))

      try {
        while (true) {
          Thread.sleep(2000)  // Stream will continue to be consumed in the background
        }
      } catch {
        case _: InterruptedException =>
          println("[INFO]  Received Interrupted Exception. Shutting down")
        case e: Throwable =>
          println(s"[ERROR] $e")
          e.printStackTrace()
      } finally {
        shutdown()
      }
    }

    def shutdown(): Unit = {
      println("[INFO]  Cleaning up")
      Util.twitterStream.cleanUp
      println("[INFO]  Shutting down")
      Util.twitterStream.shutdown
      Util.kafkaProducer.close()
    }
  }

  val twitterStreamRunnable: TwitterStreamRunnable = new TwitterStreamRunnable
  val thread: Thread = new Thread(twitterStreamRunnable)

  sys.addShutdownHook({
    println("[INFO]  Shutting down thread...")
    twitterStreamRunnable.shutdown()
    thread.interrupt()
  })

  thread.start()
}
