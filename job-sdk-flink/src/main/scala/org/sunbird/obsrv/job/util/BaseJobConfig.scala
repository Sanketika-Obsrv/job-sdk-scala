package org.sunbird.obsrv.job.util

import com.typesafe.config.Config
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import scala.collection.mutable
import java.io.Serializable
import java.util.Properties

abstract class BaseJobConfig[T](val config: Config, val jobName: String) extends Serializable {

  private val serialVersionUID = -4515020556926788923L

  implicit val metricTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  lazy val defaultDatasetID: String = "ALL"

  private val kafkaProducerBrokerServers: String = config.getString("kafka.producer.broker-servers")
  private val kafkaConsumerBrokerServers: String = config.getString("kafka.consumer.broker-servers")
  // Producer Properties
  private val kafkaProducerMaxRequestSize: Int = config.getInt("kafka.producer.max-request-size")
  private val kafkaProducerBatchSize: Int = config.getInt("kafka.producer.batch.size")
  private val kafkaProducerLingerMs: Int = config.getInt("kafka.producer.linger.ms")
  private val kafkaProducerCompression: String = if (config.hasPath("kafka.producer.compression")) config.getString("kafka.producer.compression") else "snappy"
  private val groupId: String = config.getString("kafka.groupId")
  val restartAttempts: Int = config.getInt("task.restart-strategy.attempts")
  val delayBetweenAttempts: Long = config.getLong("task.restart-strategy.delay")
  val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  val downstreamOperatorsParallelism: Int = config.getInt("task.downstream.operators.parallelism")
  val waterMarkTimeBound: Int = config.getInt("task.waterMark.timeBound")
  // Only for Tests
  private val kafkaAutoOffsetReset: Option[String] = if (config.hasPath("kafka.auto.offset.reset")) Option(config.getString("kafka.auto.offset.reset")) else None

  // Redis
  val redisHost: String = Option(config.getString("redis.host")).getOrElse("localhost")
  val redisPort: Int = Option(config.getInt("redis.port")).getOrElse(6379)
  val redisConnectionTimeout: Int = Option(config.getInt("redis.connection.timeout")).getOrElse(30000)

  val systemEventCount = "system-event-count"
  val kafkaSystemTopic: String = config.getString("kafka.output.system.event.topic")
  private val SYSTEM_EVENTS_OUTPUT_TAG = "system-events"
  val systemEventsOutputTag: OutputTag[String] = OutputTag[String](SYSTEM_EVENTS_OUTPUT_TAG)
  val systemEventsProducer = "system-events-sink"

  // Checkpointing config
  val enableCompressedCheckpointing: Boolean = if (config.hasPath("job.enable.distributed.checkpointing")) config.getBoolean("job.enable.distributed.checkpointing") else false
  val checkpointingInterval: Int = config.getInt("task.checkpointing.interval")
  val checkpointingPauseSeconds: Int = config.getInt("task.checkpointing.pause.between.seconds")
  val checkpointingStateBackendType: String = if (config.hasPath("job.statebackend.type")) config.getString("job.statebackend.type") else "hashmap"
  val enableDistributedCheckpointing: Option[Boolean] = if (config.hasPath("job.enable.distributed.checkpointing")) Option(config.getBoolean("job.enable.distributed.checkpointing")) else None
  val checkpointingBaseUrl: Option[String] = if (config.hasPath("job.statebackend.base.url")) Option(config.getString("job.statebackend.base.url")) else None

  // Base Methods
  def inputTopic(): String

  def inputConsumer(): String

  def successTag(): OutputTag[T]

  // Event Failures Common Variables
  val failedEventProducer = "failed-events-sink"
  val eventFailedMetricsCount: String = "failed-event-count"
  val kafkaFailedTopic: String = config.getString("kafka.output.failed.topic")
  def failedEventsOutputTag(): OutputTag[T]

  def kafkaConsumerProperties(kafkaBrokerServers: Option[String] = None, kafkaConsumerGroup: Option[String] = None): Properties = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", kafkaBrokerServers.getOrElse(kafkaConsumerBrokerServers))
    properties.setProperty("group.id", kafkaConsumerGroup.getOrElse(groupId))
    properties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
    kafkaAutoOffsetReset.map {
      properties.setProperty("auto.offset.reset", _)
    }
    properties
  }

  def kafkaProducerProperties: Properties = {
    val properties = new Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProducerBrokerServers)
    properties.put(ProducerConfig.LINGER_MS_CONFIG, Integer.valueOf(kafkaProducerLingerMs))
    properties.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.valueOf(kafkaProducerBatchSize))
    properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, kafkaProducerCompression)
    properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, Integer.valueOf(kafkaProducerMaxRequestSize))
    properties
  }

  // String Constants
  val CONST_OBSRV_META = "obsrv_meta"
  val CONST_DATASET = "dataset"
  val CONST_EVENT = "event"

  // extract ets for watermark timestamping
  private val timestampAssigner = new SerializableTimestampAssigner[mutable.Map[String, AnyRef]] {
    override def extractTimestamp(event: mutable.Map[String, AnyRef], recordTimestamp: Long): Long = {
      val ets: Long = event.get("ets") match {
        case Some(value: java.lang.Double) => value.toLong
        case Some(value: java.lang.Long) => value
        case _ => recordTimestamp
      }
      ets
    }
  }
  // create watermark with event.ets timestamp
  val watermarkStrategy: WatermarkStrategy[mutable.Map[String, AnyRef]] =
    WatermarkStrategy
      .forBoundedOutOfOrderness(java.time.Duration.ofSeconds(waterMarkTimeBound))
      .withTimestampAssigner(timestampAssigner)
  // assign watermark to stream

}
