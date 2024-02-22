package org.sunbird.obsrv.job.exception

import org.sunbird.obsrv.job.model.Models.ErrorData

class ObsrvException(val error: ErrorData) extends Exception(error.errorMsg) {

}
