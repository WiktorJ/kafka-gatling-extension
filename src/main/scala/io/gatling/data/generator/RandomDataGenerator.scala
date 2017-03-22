package io.gatling.data.generator

import java.nio.ByteBuffer

import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericData, GenericRecord}

import scala.collection.JavaConverters._
import scala.util.Random

class RandomDataGenerator[K: Manifest, V: Manifest] {
  private final val ByteManifest = manifest[Byte]
  private final val ShortManifest = manifest[Short]
  private final val IntManifest = manifest[Int]
  private final val LongManifest = manifest[Long]
  private final val FloatManifest = manifest[Float]
  private final val DoubleManifest = manifest[Double]
  private final val CharManifest = manifest[Char]
  private final val StringManifest = manifest[String]
  private final val BooleanManifest = manifest[Boolean]
  private final val ByteArrayManifest = manifest[Array[Byte]]
  private final val AnyManifest = manifest[Any]
  private final val AnyRefManifest = manifest[AnyRef]
  private final val GenericRecordManifest = manifest[GenericRecord]
  private final val genericData = new GenericData();
  def generateKey(schema: Option[Schema] = None, byteDataSize: () => Int = () => 100): K = {
    genByManifest[K](schema, byteDataSize)
  }

  def generateValue(schema: Option[Schema] = None, byteDataSize: () => Int = () => 100): V = {
    genByManifest[V](schema, byteDataSize)
  }

  protected def genByManifest[Z:Manifest](schema: Option[Schema], byteDataSize: () => Int)= {
    manifest[Z] match {
      case ByteManifest => 42.toByte.asInstanceOf[Z]
      case ShortManifest => 42.toShort.asInstanceOf[Z]
      case IntManifest => 42.asInstanceOf[Z]
      case LongManifest => 42.toLong.asInstanceOf[Z]
      case FloatManifest => 42.11.toFloat.asInstanceOf[Z]
      case DoubleManifest => 42.11.asInstanceOf[Z]
      case CharManifest => 'C'.asInstanceOf[Z]
      case StringManifest => "Str".asInstanceOf[Z]
      case BooleanManifest => true.asInstanceOf[Z]
      case ByteArrayManifest => generateRandomByteData(byteDataSize).asInstanceOf[Z]
      case AnyManifest => 'C'.asInstanceOf[Z]
      case AnyRefManifest => "Str".asInstanceOf[Z]
      case GenericRecordManifest => generateDataForAvroSchema(schema).asInstanceOf[Z]
      case x if x.runtimeClass.isArray =>
        Array(1, 2, 3, 4, 5).asInstanceOf[Z]
    }
  }

  protected def generateRandomByteData(f: () => Int): Array[Byte] = {
    val arr: Array[Byte] = new Array[Byte](f())
    Random.nextBytes(arr)
    arr
  }


  protected def generateDataForAvroSchema(schemaOpt: Option[Schema]): Any = {
    schemaOpt match {
      case None => throw new RuntimeException("schema is empty. Cannot generate record")
      case Some(schema) =>
        val fieldType = schema.getType
        fieldType match {
          case Type.NULL => null
          case Type.BOOLEAN => true
          case Type.ENUM => genericData.createEnum(schema.getEnumSymbols.get(0), schema)
          case Type.INT => 42
          case Type.LONG => 42.toLong
          case Type.FLOAT => 42.11.toFloat
          case Type.DOUBLE => 42.11
          case Type.FIXED => new GenericData.Fixed(schema, Array.range(0, schema.getFixedSize).map(_.toByte) )
          case Type.STRING => "42"
          case Type.BYTES => ByteBuffer.wrap("42".getBytes)
          case Type.RECORD => {
            val avroRecord = new Record(schema)
            schema.getFields.asScala.foreach(
              field => {
                avroRecord.put(field.name(), generateDataForAvroSchema(Some(field.schema())))
              })
            avroRecord
          }
          case Type.ARRAY => Array.range(0,3).map(_ => generateDataForAvroSchema(Some(schema.getElementType))).toList.asJava
          case Type.MAP => Array.range(0,3).zipWithIndex.map(tuple => (tuple._1.toString(), generateDataForAvroSchema(Some(schema.getValueType())))).toMap.asJava // maps must have a string as the key
          case Type.UNION => generateDataForAvroSchema(schema.getTypes.asScala.filter(s => s.getType() != Type.NULL).headOption) // return first non null type
        }
    }
  }
}