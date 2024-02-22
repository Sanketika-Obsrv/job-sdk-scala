package org.sunbird.obsrv.job.util

import com.typesafe.config.Config
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}

import java.nio.charset.StandardCharsets
import java.util.Properties

class FlinkKafkaConnector(config: Config) {

  def kafkaSink[T](kafkaTopic: String): KafkaSink[T] = {
    KafkaSink.builder[T]()
      .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .setRecordSerializer(new SerializationSchema(kafkaTopic))
      .setKafkaProducerConfig(kafkaProducerProperties)
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

}

class SerializationSchema[T](topic: String) extends KafkaRecordSerializationSchema[T] {

  private val serialVersionUID = -4284080856874185929L

  override def serialize(element: T, context: KafkaRecordSerializationSchema.KafkaSinkContext, timestamp: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val out = JSONUtil.serialize(element)
    new ProducerRecord[Array[Byte], Array[Byte]](topic, out.getBytes(StandardCharsets.UTF_8))
  }
}