package org.sunbird.obsrv.job.function

import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.util.Collector
import org.sunbird.obsrv.job.util.Metrics

import java.lang
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable

abstract class BaseWindowProcessFunction[I, O, K, W <: Window] extends ProcessWindowFunction[I, O, K, W] with SystemEventHandler {

  protected val metrics: Metrics = Metrics(mutable.Map[String, ConcurrentHashMap[String, AtomicLong]]())

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  def getMetrics(): List[String]

  def initMetrics(connectorId: String, connectorInstanceId: String): Unit = {
    if (!metrics.hasJob(connectorInstanceId)) {
      val metricMap = new ConcurrentHashMap[String, AtomicLong]()
      getMetrics().map(metric => {
        metricMap.put(metric, new AtomicLong(0L))
        getRuntimeContext.getMetricGroup.addGroup(connectorId).addGroup(connectorInstanceId)
          .gauge[Long, ScalaGauge[Long]](metric, ScalaGauge[Long](() => metrics.getAndReset(connectorInstanceId, metric)))
      })
      metrics.initJob(connectorInstanceId, metricMap)
    }
  }

  def process(key: K, context: ProcessWindowFunction[I, O, K, W]#Context, elements: lang.Iterable[I], metrics: Metrics): Unit

  override def process(key: K, context: ProcessWindowFunction[I, O, K, W]#Context, elements: lang.Iterable[I], out: Collector[O]): Unit = {
    process(key, context, elements, metrics)
  }

}