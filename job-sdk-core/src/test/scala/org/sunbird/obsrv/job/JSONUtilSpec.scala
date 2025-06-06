package org.sunbird.obsrv.job

import org.scalatest.Matchers.{be, convertToAnyShouldWrapper}
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import org.sunbird.obsrv.job.util.JSONUtil

class JSONUtilSpec extends FlatSpec with BeforeAndAfterAll {

  "JSONUtilSpec" should s"test the isJSON methods for all conditions" in {

    JSONUtil.isJson("") should be(false)
    JSONUtil.isJson("null") should be(false)
    JSONUtil.isJson("text") should be(false)
    JSONUtil.isJson(null) should be(false)
    JSONUtil.isJson("""{""") should be(false)
    JSONUtil.isJson("""{}""") should be(true)
    JSONUtil.isJson("""[{}]""") should be(true)
    JSONUtil.isJson("""2""") should be(false)
    JSONUtil.isJson("""false""") should be(false)

    JSONUtil.isJsonObject("""{}""") should be(true)
    JSONUtil.isJsonObject("""[{}]""") should be(false)
  }
}
