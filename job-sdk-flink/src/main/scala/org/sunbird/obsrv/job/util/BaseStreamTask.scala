package org.sunbird.obsrv.job.util

import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink, KeyedStream, SideOutputDataStream, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import scala.collection.mutable

class BaseStreamTaskSink[T] {
  def addDefaultSinks(dataStream: SingleOutputStreamOperator[T], config: BaseJobConfig[T], kafkaConnector: FlinkKafkaConnector): DataStreamSink[T] = {

    dataStream.getSideOutput(config.systemEventsOutputTag).sinkTo(kafkaConnector.kafkaSink[String](config.kafkaSystemTopic))
      .name(config.jobName + "-" + config.systemEventsProducer).uid(config.jobName + "-" + config.systemEventsProducer).setParallelism(config.downstreamOperatorsParallelism)

    dataStream.getSideOutput(config.failedEventsOutputTag()).sinkTo(kafkaConnector.kafkaSink[T](config.kafkaFailedTopic))
      .name(config.jobName + "-" + config.failedEventProducer).uid(config.jobName + "-" + config.failedEventProducer).setParallelism(config.downstreamOperatorsParallelism)
  }
}

abstract class BaseKeyedStreamTask[T, K] extends BaseStreamTaskSink[T] {

  def processStream(keyedStream: KeyedStream[T, K]): SideOutputDataStream[mutable.Map[String, AnyRef]]

  def getMapDataStream(env: StreamExecutionEnvironment, config: BaseJobConfig[T], kafkaConnector: FlinkKafkaConnector): DataStream[mutable.Map[String, AnyRef]] = {
    env.fromSource(kafkaConnector.kafkaMapSource(config.inputTopic()), config.watermarkStrategy, config.inputConsumer())
      .uid(config.inputConsumer()).setParallelism(config.kafkaConsumerParallelism)
      .rebalance()
  }

}