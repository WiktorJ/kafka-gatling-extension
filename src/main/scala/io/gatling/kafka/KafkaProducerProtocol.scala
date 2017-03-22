package io.gatling.kafka

import io.gatling.core.Predef.Session
import io.gatling.core.protocol.Protocol
import io.gatling.data.generator.RandomDataGenerator
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

class KafkaProducerProtocol[K: Manifest, V: Manifest](props: java.util.HashMap[String, Object],
                                  topics: Array[String], numberOfProducers: Int,
                                  dataGenerator: RandomDataGenerator[K, V] = null)
  extends Protocol {

  private val kafkaProducers = new Array[KafkaProducer[K, V]](numberOfProducers)
  for ( i <- 0 until numberOfProducers) {
    kafkaProducers(i) = new KafkaProducer[K, V](props)
  }
  println(s"Created kafkaProducer with $props")
  private var key: K = _
  private var value: V = _

  def call(session: Session,
           schema: Option[Schema] = None, byteDataSize: () => Int): Unit = {
    val attributes = session.attributes

    if (attributes.nonEmpty) {
      if (manifest[K].runtimeClass.isArray &&
        manifest[V].runtimeClass.isArray) {
        key = attributes.toString().getBytes().asInstanceOf[K]
        value = attributes.toString().getBytes().asInstanceOf[V]
      } else {
        key = createRecordForAvroSchema(attributes).asInstanceOf[K]
        value = createRecordForAvroSchema(attributes).asInstanceOf[V]
      }
    } else {
        key = dataGenerator.generateKey(schema, byteDataSize)
        value = dataGenerator.generateValue(schema, byteDataSize)
    }
    if (!topics.isEmpty) {
      val record = new ProducerRecord[K, V](topics(Random.nextInt(topics.length)), key, value)
      val result = kafkaProducers(Random.nextInt(kafkaProducers.length)).send(record)
    }
//
//    for( topic  <- topics) {
//      val record = new ProducerRecord[K, V](topic, key, value)
//      val result = kafkaProducer.send(record)
//    }

  }

  private def createRecordForAvroSchema(attributes: Map[String, Any]): GenericRecord = {
    if (attributes.isEmpty) {
      throw new RuntimeException("attributes is empty. Cannot generate record")
    }

    val length = attributes.size

    var schemaBuilder = SchemaBuilder.record("testdata")
        .namespace("org.apache.avro").fields()

    for ((key, value) <- attributes) {
      schemaBuilder = schemaBuilder
        .name(key).`type`().nullable().stringType().noDefault()
    }
    val schema = schemaBuilder.endRecord()

    val avroRecord = new Record(schema)

    var count = 0
    for ((key, value) <- attributes) {
      avroRecord.put(count, value.toString)
      count += 1
    }
    avroRecord
  }
}