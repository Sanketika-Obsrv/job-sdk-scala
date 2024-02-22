package org.sunbird.obsrv.job.util

import com.typesafe.config.Config
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}

object FlinkUtil {

  def getExecutionContext(config: Config): StreamExecutionEnvironment = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setUseSnapshotCompression(if (config.hasPath("job.enable.distributed.checkpointing")) config.getBoolean("job.enable.distributed.checkpointing") else false)
    env.enableCheckpointing(config.getInt("task.checkpointing.interval"))
    if (config.hasPath("job.enable.distributed.checkpointing")) {
      val checkpointingBaseUrl: Option[String] = if (config.hasPath("job.statebackend.base.url")) Option(config.getString("job.statebackend.base.url")) else None
      env.setStateBackend(new HashMapStateBackend())
      val checkpointConfig: CheckpointConfig = env.getCheckpointConfig
      checkpointConfig.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
      checkpointConfig.setMinPauseBetweenCheckpoints(config.getInt("task.checkpointing.pause.between.seconds"))
      checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
      checkpointConfig.setCheckpointStorage(s"${checkpointingBaseUrl.getOrElse("")}/${config.getString("connector.metadata.id")}")
    }
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(config.getInt("task.restart-strategy.attempts"), config.getLong("task.restart-strategy.delay")))
    env
  }

}
