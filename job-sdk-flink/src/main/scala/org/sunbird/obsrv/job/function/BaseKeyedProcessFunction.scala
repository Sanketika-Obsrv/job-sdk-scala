package org.sunbird.obsrv.job.function

import org.sunbird.obsrv.job.util.{BaseFunction, JobMetrics, Metrics}
import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

abstract class BaseKeyedProcessFunction[K, I, O] extends KeyedProcessFunction[K, I, O] with SystemEventHandler with JobMetrics with BaseFunction {

  protected val metrics: Metrics = Metrics(mutable.Map[String, ConcurrentHashMap[String, AtomicLong]]())

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  def processElement(event: I, context: KeyedProcessFunction[K, I, O]#Context, metrics: Metrics): Unit

  override def processElement(event: I, context: KeyedProcessFunction[K, I, O]#Context, out: Collector[O]): Unit = {

    processElement(event, context, metrics)
  }

  def onTimer(timestamp: Long, context: KeyedProcessFunction[K, I, O]#OnTimerContext, metrics: Metrics): Unit
  override def onTimer(timestamp: Long, context: KeyedProcessFunction[K, I, O]#OnTimerContext, out: Collector[O]): Unit = {
    onTimer(timestamp, context, metrics)
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

}