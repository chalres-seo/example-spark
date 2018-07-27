package com.example.utils

import java.io.{File, FileInputStream}
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable

object AppConfig {
  // read application.conf
  private val conf: Config = ConfigFactory.parseFile(new File("conf/application.conf")).resolve()

  // read spark.conf
  private val sparkProps: Properties = new Properties()
  sparkProps.load(new FileInputStream("conf/spark.conf"))

  // retry conf
  val backoffTimeInMillis: Long = conf.getLong("retry.backoffTimeInMillis")
  val attemptMaxCount: Int = conf.getInt("retry.attemptMaxCount")

  // aws account config
  val awsProfile: String = conf.getString("aws.profile")
  val awsRegion: String = conf.getString("aws.region")
  val awsEndpoint: String = conf.getString("aws.endpoint")

  // spark config
  val sparkAppName: String = conf.getString("spark.appName")
  val sparkConfig: mutable.Map[String, String] = sparkProps.asScala
  val sparkPalleleLevel: Int = Runtime.getRuntime.availableProcessors() * 2
}