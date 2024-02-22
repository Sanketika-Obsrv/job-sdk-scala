package org.sunbird.obsrv.job.util

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable

case class Metrics(metrics: mutable.Map[String, ConcurrentHashMap[String, AtomicLong]]) {

  private def getMetric(id: String, metric: String): AtomicLong = {
    val datasetMetrics: ConcurrentHashMap[String, AtomicLong] = metrics.getOrElse(id, new ConcurrentHashMap[String, AtomicLong]())
    datasetMetrics.getOrDefault(metric, new AtomicLong())
  }

  def hasJob(jobId: String): Boolean = {
    metrics.contains(jobId)
  }

  def initJob(jobId: String, counters: ConcurrentHashMap[String, AtomicLong]): Unit = {
    metrics.put(jobId, counters)
  }

  def incCounter(id: String, metric: String): Unit = {
    getMetric(id, metric).getAndIncrement()
  }

  def incCounter(id: String, metric: String, count: Long): Unit = {
    getMetric(id, metric).getAndAdd(count)
  }

  def getAndReset(dataset: String, metric: String): Long = {
    getMetric(dataset, metric).getAndSet(0L)
  }

  def get(dataset: String, metric: String): Long = {
    getMetric(dataset, metric).get()
  }

  def reset(dataset: String, metric: String): Unit = getMetric(dataset, metric).set(0L)
}
