package org.sunbird.obsrv.job.util

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, OffsetResetStrategy}
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets
import java.util.Properties

class FlinkKafkaConnector(config: Config) extends Serializable {

  def kafkaSink[T](kafkaTopic: String): KafkaSink[T] = {

    KafkaSink.builder[T]()
      .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .setRecordSerializer(new SerializationSchema(kafkaTopic))
      .setKafkaProducerConfig(kafkaProducerProperties)
      .build()
  }

  def kafkaStringSource(kafkaTopic: String, connectorInstanceId: String): KafkaSource[String] = {
    KafkaSource.builder[String]()
      .setTopics(kafkaTopic)
      .setDeserializer(new StringDeserializationSchema)
      .setProperties(kafkaConsumerProperties(connectorInstanceId))
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
      .build()
  }

  private def kafkaProducerProperties: Properties = {
    val properties = new Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.producer.broker-servers"))
    properties.put(ProducerConfig.LINGER_MS_CONFIG, Integer.valueOf(config.getInt("kafka.producer.linger.ms")))
    properties.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.valueOf(config.getInt("kafka.producer.batch.size")))
    properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, if (config.hasPath("kafka.producer.compression")) config.getString("kafka.producer.compression") else "snappy")
    properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, Integer.valueOf(config.getInt("kafka.producer.max-request-size")))
    properties
  }

  private def kafkaConsumerProperties(connectorInstanceId: String): Properties = {
    val properties = new Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.producer.broker-servers"))
    properties.setProperty("group.id", connectorInstanceId)
    properties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
    properties.setProperty("auto.offset.reset", if (config.hasPath("connector_auto_offset_reset")) config.getString("connector_auto_offset_reset") else "earliest")
    properties
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