package org.sunbird.obsrv.job.util

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetResetStrategy}
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets
import java.util.Properties
import scala.collection.mutable
import scala.collection.JavaConverters._

class FlinkKafkaConnector(config: BaseJobConfig[_]) {

  def kafkaSink[T](kafkaTopic: String): KafkaSink[T] = {
    KafkaSink.builder[T]()
      .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .setRecordSerializer(new SerializationSchema(kafkaTopic))
      .setKafkaProducerConfig(config.kafkaProducerProperties)
      .build()
  }

  def kafkaStringSource(kafkaTopic: String, connectorInstanceId: String): KafkaSource[String] = {
    KafkaSource.builder[String]()
      .setTopics(kafkaTopic)
      .setDeserializer(new StringDeserializationSchema)
      .setProperties(config.kafkaConsumerProperties())
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
      .build()
  }

  def kafkaMapSource(kafkaTopic: String): KafkaSource[mutable.Map[String, AnyRef]] = {
    kafkaMapSource(List(kafkaTopic), config.kafkaConsumerProperties())
  }

  def kafkaMapSource(kafkaTopics: List[String], consumerProperties: Properties): KafkaSource[mutable.Map[String, AnyRef]] = {
    KafkaSource.builder[mutable.Map[String, AnyRef]]()
      .setTopics(kafkaTopics.asJava)
      .setDeserializer(new MapDeserializationSchema)
      .setProperties(consumerProperties)
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
      .build()
  }

}

class SerializationSchema[T](topic: String) extends KafkaRecordSerializationSchema[T] {

  private val serialVersionUID = -4284080856874185929L

  override def serialize(element: T, context: KafkaRecordSerializationSchema.KafkaSinkContext, timestamp: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val out = JSONUtil.serialize(element)
    new ProducerRecord[Array[Byte], Array[Byte]](topic, out.getBytes(StandardCharsets.UTF_8))
  }
}

class StringDeserializationSchema extends KafkaRecordDeserializationSchema[String] {

  private val serialVersionUID = -3224825136576915426L
  private[this] val logger = LoggerFactory.getLogger(classOf[StringDeserializationSchema])

  override def getProducedType: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]], out: Collector[String]): Unit = {
    try {
      out.collect(new String(record.value(), StandardCharsets.UTF_8))
    } catch {
      case ex: NullPointerException =>
        logger.error(s"Exception while parsing the message: ${ex.getMessage}")
    }
  }
}