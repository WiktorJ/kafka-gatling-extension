package io.gatling.simulation

import java.util

import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.gatling.core.Predef.{Simulation, _}
import io.gatling.data.generator.RandomDataGenerator
import io.gatling.kafka.{KafkaProducerBuilder, KafkaProducerProtocol}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.ProducerConfig

import scala.concurrent.duration._

class SimulationWithAvroSchema extends Simulation {
  val kafkaTopic = Array("test_topic")
  val kafkaBrokers = "localhost:9092"

  val props = new util.HashMap[String, Object]()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
  props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000")
  props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000")

  props.put("schema.registry.url", "http://localhost:8081")

  val video_schema =
    s"""
       |{
       |  "type": "record",
       |  "name": "VideoClaimabilityInput",
       |  "namespace": "com.zefr.avro.video",
       |  "doc": "Event format for video claimability",
       |  "fields": [
       |    {
       |      "name": "uuid",
       |      "type": [
       |        "null",
       |        "string"
       |      ],
       |      "doc": "Globally unique identifier for the event across all topics and messages",
       |      "default": null
       |    },
       |    {
       |      "name": "category",
       |      "type": [
       |        "null",
       |        "string"
       |      ],
       |      "doc": "Unique category for the event across all topics",
       |      "default": null
       |    },
       |    {
       |      "name": "date",
       |      "type": [
       |        "null",
       |        "string"
       |      ],
       |      "doc": "ISO-8601 date when event was created",
       |      "default": null
       |    },
       |    {
       |      "name": "entity_id",
       |      "type": [
       |        "null",
       |        "string"
       |      ],
       |      "doc": "Identifier for the entity that is referred to in the event",
       |      "default": null
       |    },
       |    {
       |      "name": "source",
       |      "type": [
       |        "null",
       |        "string"
       |      ],
       |      "doc": "Name of the source app",
       |      "default": null
       |    },
       |    {
       |      "name": "payload",
       |      "type": [
       |        "null",
       |        {
       |          "type": "record",
       |          "name": "VideoClaimabilityInputPayload",
       |          "doc": "The set of inputs required to predict wether a video can be claimed",
       |          "fields": [
       |            {
       |              "name": "title_id",
       |              "type": [
       |                "null",
       |                "string"
       |              ],
       |              "doc": "Id of the title to be tested against",
       |              "default": null
       |            },
       |            {
       |              "name": "video",
       |              "type": [
       |                "null",
       |                {
       |                  "type": "record",
       |                  "name": "Video",
       |                  "doc": "A cross platform video metadata holder",
       |                  "fields": [
       |                    {
       |                      "name": "platform",
       |                      "type": [
       |                        "null",
       |                        {
       |                          "type": "enum",
       |                          "name": "VideoPlatform",
       |                          "doc": "We use 2 letter acronyms for no good reason???",
       |                          "symbols": [
       |                            "YT",
       |                            "FB"
       |                          ]
       |                        }
       |                      ],
       |                      "doc": "Platform the video is from",
       |                      "default": null
       |                    },
       |                    {
       |                      "name": "content_id",
       |                      "type": [
       |                        "null",
       |                        "string"
       |                      ],
       |                      "doc": "Content id for the video",
       |                      "default": null
       |                    },
       |                    {
       |                      "name": "title",
       |                      "type": [
       |                        "null",
       |                        "string"
       |                      ],
       |                      "doc": "Title of the video",
       |                      "default": null
       |                    },
       |                    {
       |                      "name": "description",
       |                      "type": [
       |                        "null",
       |                        "string"
       |                      ],
       |                      "doc": "Long description of the title",
       |                      "default": null
       |                    },
       |                    {
       |                      "name": "channel_id",
       |                      "type": [
       |                        "null",
       |                        "string"
       |                      ],
       |                      "doc": "Channel where the video is from",
       |                      "default": null
       |                    },
       |                    {
       |                      "name": "channel_title",
       |                      "type": [
       |                        "null",
       |                        "string"
       |                      ],
       |                      "doc": "Title of the channel",
       |                      "default": null
       |                    },
       |                    {
       |                      "name": "published_at",
       |                      "type": [
       |                        "null",
       |                        "string"
       |                      ],
       |                      "doc": "Publish time in milliseconds since 1970",
       |                      "default": null
       |                    },
       |                    {
       |                      "name": "duration",
       |                      "type": [
       |                        "null",
       |                        "int"
       |                      ],
       |                      "doc": "Duration in ???",
       |                      "default": null
       |                    },
       |                    {
       |                      "name": "category_id",
       |                      "type": [
       |                        "null",
       |                        "string"
       |                      ],
       |                      "doc": "The id of category the video is in",
       |                      "default": null
       |                    },
       |                    {
       |                      "name": "dimension",
       |                      "type": [
       |                        "null",
       |                        {
       |                          "type": "enum",
       |                          "name": "VideoDimension",
       |                          "doc": "Wether this video requires special goggles???",
       |                          "symbols": [
       |                            "_2D",
       |                            "_3D"
       |                          ]
       |                        }
       |                      ],
       |                      "doc": "Whether this requires special 3d glasses???",
       |                      "default": null
       |                    },
       |                    {
       |                      "name": "definition",
       |                      "type": [
       |                        "null",
       |                        {
       |                          "type": "enum",
       |                          "name": "VideoDefinition",
       |                          "doc": "Video quality enum",
       |                          "symbols": [
       |                            "SD",
       |                            "HD"
       |                          ]
       |                        }
       |                      ],
       |                      "doc": "Quality level of the video",
       |                      "default": null
       |                    },
       |                    {
       |                      "name": "licensed_content",
       |                      "type": [
       |                        "null",
       |                        "boolean"
       |                      ],
       |                      "doc": "Licensed content according to youtube???",
       |                      "default": null
       |                    },
       |                    {
       |                      "name": "yt_age_restricted",
       |                      "type": [
       |                        "null",
       |                        "string"
       |                      ],
       |                      "doc": "Does the user have to lie about their age???",
       |                      "default": null
       |                    },
       |                    {
       |                      "name": "topic_ids",
       |                      "type": [
       |                        "null",
       |                        {
       |                          "type": "array",
       |                          "items": "string"
       |                        }
       |                      ],
       |                      "doc": "A list of topic ids the video is in???",
       |                      "default": null
       |                    },
       |                    {
       |                      "name": "relevant_topic_ids",
       |                      "type": [
       |                        "null",
       |                        {
       |                          "type": "array",
       |                          "items": "string"
       |                        }
       |                      ],
       |                      "doc": "A list of relevant topic ids the video is in??? How is this different than topic_ids???",
       |                      "default": null
       |                    },
       |                    {
       |                      "name": "metrics",
       |                      "type": [
       |                        "null",
       |                        {
       |                          "type": "record",
       |                          "name": "VideoMetrics",
       |                          "doc": "An aggregate holder of information regarding the popularity of a video",
       |                          "fields": [
       |                            {
       |                              "name": "views",
       |                              "type": [
       |                                "null",
       |                                "long"
       |                              ],
       |                              "doc": "Total number of views",
       |                              "default": null
       |                            },
       |                            {
       |                              "name": "likes",
       |                              "type": [
       |                                "null",
       |                                "long"
       |                              ],
       |                              "doc": "Total number of likes",
       |                              "default": null
       |                            },
       |                            {
       |                              "name": "comments",
       |                              "type": [
       |                                "null",
       |                                "long"
       |                              ],
       |                              "doc": "Total number of comments",
       |                              "default": null
       |                            },
       |                            {
       |                              "name": "favorites",
       |                              "type": [
       |                                "null",
       |                                "long"
       |                              ],
       |                              "doc": "Total number of favorites",
       |                              "default": null
       |                            },
       |                            {
       |                              "name": "dislikes",
       |                              "type": [
       |                                "null",
       |                                "long"
       |                              ],
       |                              "doc": "Total number of dislikes",
       |                              "default": null
       |                            },
       |                            {
       |                              "name": "reactions",
       |                              "type": [
       |                                "null",
       |                                "long"
       |                              ],
       |                              "doc": "Total number of reactions",
       |                              "default": null
       |                            }
       |                          ]
       |                        }
       |                      ],
       |                      "doc": "A set of video metrics",
       |                      "default": null
       |                    },
       |                    {
       |                      "name": "view_url",
       |                      "type": [
       |                        "null",
       |                        "string"
       |                      ],
       |                      "doc": "different from video_url???",
       |                      "default": null
       |                    },
       |                    {
       |                      "name": "video_url",
       |                      "type": [
       |                        "null",
       |                        "string"
       |                      ],
       |                      "doc": "different from view_url???",
       |                      "default": null
       |                    },
       |                    {
       |                      "name": "from_related_video",
       |                      "type": [
       |                        "null",
       |                        "string"
       |                      ],
       |                      "doc": "???",
       |                      "default": null
       |                    },
       |                    {
       |                      "name": "last_update_time",
       |                      "type": [
       |                        "null",
       |                        "string"
       |                      ],
       |                      "doc": "Last time the video was updated",
       |                      "default": null
       |                    }
       |                  ]
       |                }
       |              ],
       |              "doc": "Video information to be tested against",
       |              "default": null
       |            }
       |          ]
       |        }
       |      ],
       |      "doc": "Information specific to video claimability",
       |      "default": null
       |    }
       |  ]
       |}
     """.stripMargin

  val schema = new Schema.Parser().parse(video_schema)

  val dataGenerator = new RandomDataGenerator[String, GenericRecord]()
  val kafkaProducerProtocol = new KafkaProducerProtocol[String, GenericRecord](props, kafkaTopic, 1, dataGenerator)
  val scn = scenario("Kafka Producer Call").exec(KafkaProducerBuilder[String, GenericRecord](Some(schema)))

  // constantUsersPerSec(100000) during (1 minute)
  // apple macbook pro can do ~30000/second
  setUp(scn.inject(constantUsersPerSec(10000) during (60 seconds))).protocols(kafkaProducerProtocol)
}