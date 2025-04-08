package org.sunbird.obsrv.job.util

import scala.collection.mutable

object Util {

  def time[R](block: => R): (Long, R) = {
    val t0 = System.currentTimeMillis()
    val result = block // call-by-name
    val t1 = System.currentTimeMillis()
    ((t1 - t0), result)
  }

  def getMutableMap(immutableMap: Map[String, AnyRef]): mutable.Map[String, AnyRef] = {
    val mutableMap = mutable.Map[String, AnyRef]()
    mutableMap ++= immutableMap
    mutableMap
  }

}
