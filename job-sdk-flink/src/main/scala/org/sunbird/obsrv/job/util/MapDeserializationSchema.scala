package org.sunbird.obsrv.job.util

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord
import scala.collection.mutable
import org.sunbird.obsrv.job.model.Constants

class MapDeserializationSchema extends KafkaRecordDeserializationSchema[mutable.Map[String, AnyRef]] {

  private val serialVersionUID = -3224825136576915426L

  override def getProducedType: TypeInformation[mutable.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[mutable.Map[String, AnyRef]])

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]], out: Collector[mutable.Map[String, AnyRef]]): Unit = {
    val msg = try {
      JSONUtil.deserialize[mutable.Map[String, AnyRef]](record.value())
    } catch {
      case _: Exception =>
        mutable.Map[String, AnyRef](Constants.INVALID_JSON -> new String(record.value, "UTF-8"))
    }
    initObsrvMeta(msg, record)
    out.collect(msg)
  }

  private def initObsrvMeta(msg: mutable.Map[String, AnyRef], record: ConsumerRecord[Array[Byte], Array[Byte]]): Unit = {
    if (!msg.contains("obsrv_meta")) {
      msg.put("obsrv_meta", Map(
        "syncts" -> record.timestamp(),
        "processingStartTime" -> System.currentTimeMillis(),
        "flags" -> Map(),
        "timespans" -> Map(),
        "error" -> Map(),
        "source" -> Map(
          "connector" -> "api",
          "connectorInstance" -> "api",
        )
      ))
    }
  }
}
