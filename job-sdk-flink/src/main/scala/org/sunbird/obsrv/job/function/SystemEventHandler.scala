package org.sunbird.obsrv.job.function

import org.sunbird.obsrv.job.model.Models._
import org.sunbird.obsrv.job.model.StatusCode.StatusCode
import org.sunbird.obsrv.job.model._
import org.sunbird.obsrv.job.util.JSONUtil

trait SystemEventHandler {

  def generateSystemEvent(contextData: ContextData, error: ErrorData, status: StatusCode, optParams: Option[Map[String, AnyRef]] = None): String = {

    JSONUtil.serialize(SystemEvent(
      EventID.METRIC, ctx = contextData,
      data = EData(error = Some(ErrorLog(contextData.connectorId, status, error.errorCode, error.errorCode, error.errorMsg)), extra = optParams)
    ))
  }

}
