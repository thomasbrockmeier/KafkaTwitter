package com.github.thomasbrockmeier.elasticsearchconsumer


import com.sksamuel.elastic4s.http.{ElasticClient, ElasticProperties}

object ElasticsearchConsumer extends App {
  class Util {
    private def getElasticsearch: ElasticClient = {
      val HOST = "https://8qz7l5uy6b:fcskekhft1@twitter-cluster-test-6128724752.eu-central-1.bonsaisearch.net:433"
      ElasticClient(ElasticProperties(HOST))
    }

    val elasticSearch = getElasticsearch
  }
}