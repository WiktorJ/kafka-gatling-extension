package io.gatling.simulation.utils

import io.gatling.data.generator.RandomDataGenerator
import org.apache.avro.Schema

class RandomDataGeneratorExt extends RandomDataGenerator[Int, Int] {
  var count = 0

  override def generateKey(schema: Option[Schema], f: () => Int = () => 100): Int = {
    count = count + 1
    count
  }

  override def generateValue(schema: Option[Schema], f: () => Int = () => 100): Int = {
    count = count + 1
    count
  }
}