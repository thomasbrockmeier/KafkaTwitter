package com.github.thomasbrockmeier.common

import java.io.InputStream

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import twitter4j.Status

import scala.io.Source

class Serde {
  private def getSchema(resource: String): Schema = {
    val stream: InputStream = getClass.getResourceAsStream(s"/avro/$resource.avsc")
    val avroSchema: String = Source.fromInputStream(stream).mkString
    new Schema.Parser().parse(avroSchema)
  }

  def serializeStatus(status: Status): GenericRecord = {
    new GenericRecordBuilder(tweetSchema)
      .set("created_at", status.getCreatedAt.toString)
      .set("id_str", status.getId.toString)
      .set("text", status.getText)
      .set("source", status.getSource)
      .set("retweet_count", status.getRetweetCount)
      .set("lang", status.getLang)
      .build
  }

  val tweetSchema: Schema = getSchema("tweet")
}