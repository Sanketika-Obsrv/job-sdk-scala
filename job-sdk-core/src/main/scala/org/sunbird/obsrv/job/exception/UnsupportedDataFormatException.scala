package org.sunbird.obsrv.job.exception

import org.sunbird.obsrv.job.model.Models.ErrorData

class UnsupportedDataFormatException(dataFormat: String) extends ObsrvException(ErrorData("DATA_FORMAT_ERR", s"Unsupported data format $dataFormat")) {

}
