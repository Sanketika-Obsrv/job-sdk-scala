package org.sunbird.obsrv.job.util

import com.typesafe.config.{Config, ConfigFactory}

import java.io.File

object DatasetRegistryConfig {

  private val configFile = new File("/data/flink/conf/baseconfig.conf")
  // $COVERAGE-OFF$ This code only executes within a flink cluster
  val config: Config = if (configFile.exists()) {
    println("Loading configuration file cluster baseconfig.conf...")
    ConfigFactory.parseFile(configFile).resolve()
  } else {
    println("Loading configuration file baseconfig.conf inside the jar...")
    ConfigFactory.load("baseconfig.conf").withFallback(ConfigFactory.systemEnvironment())
  }
  // $COVERAGE-ON$
  val postgresConfig = PostgresConnectionConfig(
    config.getString("postgres.user"),
    config.getString("postgres.password"),
    config.getString("postgres.database"),
    config.getString("postgres.host"),
    config.getInt("postgres.port"),
    config.getInt("postgres.maxConnections"))

}
