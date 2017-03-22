package io.gatling.data.generator

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericData
import org.scalatest.FlatSpec

/**
  * Created by ryan.barker on 2/15/17.
  */
class RandomDataGeneratorSpec extends FlatSpec{

  "A RandomDataGenerator" should "handle simple avro types" in {
    val randomDataGenerator = new RandomDataGenerator[GenericRecord, GenericRecord]
    val schema = new Schema.Parser().parse(
      """
        | {
        |    "fields": [
        |        { "name": "string1", "type": "string" },
        |        { "name": "int1", "type": "int" },
        |        { "name": "long1", "type": "long" },
        |        { "name": "float1", "type": "float" },
        |        { "name": "double1", "type": "double" },
        |        { "name": "boolean1", "type": "boolean" },
        |        { "name": "bytes1", "type": "bytes" },
        |        { "name": "null1", "type": "null" },
        |        { "name": "enum1", "type":{"type" : "enum", "name": "Colors", "symbols" : ["WHITE", "BLUE", "GREEN", "RED", "BLACK"]}},
        |        { "name": "fixed1", "type": {"type":"fixed", "name":"byte3", "size": 3 }}
        |    ],
        |    "name": "myrecord",
        |    "type": "record"
        |}
      """.stripMargin)
    val randomData = randomDataGenerator.generateKey(Some(schema))
    assert(new GenericData().validate(schema, randomData))
    assertResult ("""{"string1": "42", "int1": 42, "long1": 42, "float1": 42.11, "double1": 42.11,"""
        +""" "boolean1": true, "bytes1": {"bytes": "42"}, "null1": null, "enum1": "WHITE", "fixed1": [0, 1, 2]}""".stripMargin.stripLineEnd) {  randomData.toString}
  }


  "A RandomDataGenerator" should "handle nested records" in {
    val randomDataGenerator = new RandomDataGenerator[GenericRecord, GenericRecord]
    val schema = new Schema.Parser().parse(
      """
        | {
        |    "fields": [
        |        {
        |           "name": "record1",
        |           "type": {
        |             "name": "nestedRecord",
        |             "type":"record",
        |             "fields": [
        |                 { "name": "string1", "type": "string" }
        |             ]
        |           }
        |        }
        |    ],
        |    "name": "myrecord",
        |    "type": "record"
        |}
      """.stripMargin)
    val randomData = randomDataGenerator.generateKey(Some(schema))
    assert(new GenericData().validate(schema, randomData))
    assertResult ("""{"record1": {"string1": "42"}}""".stripMargin.stripLineEnd) {  randomData.toString}
  }

  "A RandomDataGenerator" should "handle arrays" in {
    val randomDataGenerator = new RandomDataGenerator[GenericRecord, GenericRecord]
    val schema = new Schema.Parser().parse(
      """
        | {
        |    "fields": [
        |        {
        |           "name": "array1",
        |           "type": {
        |             "name": "nestedArray",
        |             "type":"array",
        |             "items":"string"
        |           }
        |        }
        |    ],
        |    "name": "myrecord",
        |    "type": "record"
        |}
      """.stripMargin)
    val randomData = randomDataGenerator.generateKey(Some(schema))
    assert(new GenericData().validate(schema, randomData))
    assertResult ("""{"array1": ["42", "42", "42"]}""".stripMargin.stripLineEnd) {  randomData.toString}
  }

  "A RandomDataGenerator" should "handle unions" in {
    val randomDataGenerator = new RandomDataGenerator[GenericRecord, GenericRecord]
    val schema = new Schema.Parser().parse(
      """
        | {
        |    "fields": [
        |        {
        |           "name": "union1",
        |           "type": ["null","string"]
        |        }
        |    ],
        |    "name": "myrecord",
        |    "type": "record"
        |}
      """.stripMargin)
    val randomData = randomDataGenerator.generateKey(Some(schema))
    assert(new GenericData().validate(schema, randomData))
    assertResult ("""{"union1": "42"}""".stripMargin.stripLineEnd) {  randomData.toString}
  }

  "A RandomDataGenerator" should "handle maps" in {
    val randomDataGenerator = new RandomDataGenerator[GenericRecord, GenericRecord]
    val schema = new Schema.Parser().parse(
      """
        | {
        |    "fields": [
        |        {
        |           "name": "map1",
        |           "type": {
        |               "name": "nestedMap",
        |               "type": "map",
        |               "values": "int"
        |           }
        |        }
        |    ],
        |    "name": "myrecord",
        |    "type": "record"
        |}
      """.stripMargin)
    val randomData = randomDataGenerator.generateKey(Some(schema))
    assert(new GenericData().validate(schema, randomData))
    assertResult ("""{"map1": {"0": 42, "1": 42, "2": 42}}""".stripMargin.stripLineEnd) {  randomData.toString}
  }

  "A RandomDataGenerator" should "handle large nested records" in {
    val randomDataGenerator = new RandomDataGenerator[GenericRecord, GenericRecord]
    val schema = new Schema.Parser().parse(
      """
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
        |                        "long"
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
      """.stripMargin)
    val randomData = randomDataGenerator.generateKey(Some(schema))
    assert(new GenericData().validate(schema, randomData))
    assertResult ("""{"uuid": "42", "category": "42", "date": "42", "entity_id": "42", "source": "42", "payload": {"title_id": "42", """ +
      """"video": {"platform": "YT", "content_id": "42", "title": "42", "description": "42", "channel_id": "42", "channel_title": "42", """ +
      """"published_at": 42, "duration": 42, "category_id": "42", "dimension": "_2D", "definition": "SD", "licensed_content": true, """ +
      """"yt_age_restricted": "42", "topic_ids": ["42", "42", "42"], "relevant_topic_ids": ["42", "42", "42"], "metrics": {"views": 42, "likes": 42, """ +
      """"comments": 42, "favorites": 42, "dislikes": 42, "reactions": 42}, "view_url": "42", "video_url": "42", "from_related_video": "42", """ +
      """"last_update_time": "42"}}}""".stripMargin.stripLineEnd) {  randomData.toString}
  }

}
