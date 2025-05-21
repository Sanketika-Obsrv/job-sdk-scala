package org.sunbird.obsrv.job.model

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.module.scala.JsonScalaEnumeration
import org.sunbird.obsrv.job.model.EventID.EventID
import org.sunbird.obsrv.job.model.PDataType.PDataType
import org.sunbird.obsrv.job.model.StatusCode.StatusCode

object Models {

  case class ContextData(@JsonProperty("connector_id") connectorId: String, @JsonProperty("dataset_id") datasetId: String,
                         @JsonProperty("connector_instance_id") connectorInstanceId: String, @JsonProperty("connector_type") connectorType: String)

  case class ErrorLog(pdata_id: String, @JsonScalaEnumeration(classOf[StatusCodeType]) pdata_status: StatusCode, error_type: String, error_code: String, error_message: String, error_count: Option[Int] = None)

  case class EData(error: Option[ErrorLog] = None, extra: Option[Map[String, AnyRef]] = None)

  case class SystemEvent(@JsonScalaEnumeration(classOf[EventIDType]) etype: EventID, ctx: ContextData, data: EData, ets: Long = System.currentTimeMillis())

  case class ErrorData(@JsonProperty("error_code") errorCode: String, @JsonProperty("error_msg") errorMsg: String)

}

class EventIDType extends TypeReference[EventID.type]
object EventID extends Enumeration {
  type EventID = Value
  val LOG, METRIC = Value
}

class StatusCodeType extends TypeReference[StatusCode.type]
object StatusCode extends Enumeration {
  type StatusCode = Value
  val success, failed = Value
}

class PDataTypeType extends TypeReference[PDataType.type]
object PDataType extends Enumeration {
  type PDataType = Value
  val flink, spark = Value
}