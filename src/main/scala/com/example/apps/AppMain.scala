package com.example.apps

import com.example.spark.{DataFrameUtils, SparkFactory}
import com.example.utils.AppConfig
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SaveMode}

import com.databricks.spark.avro._

object AppMain extends LazyLogging {
  // Init SparkSession.
  private val spark = SparkFactory.getSparkSession

  private val _exampleDataPath = "tmp/your-example-data.csv"

  def main(args: Array[String]): Unit = {
    logger.debug(s"start spark app. name: ${AppConfig.sparkAppName}")
    logger.debug(s"spark version: ${spark.version}")

    val rawDF = DataFrameUtils.trimColumnName(this.getExampleDataFrame(_exampleDataPath))

    this.saveOrc(rawDF, "tmp/rawdata.snappy.orc", 1)
    this.saveAvro(rawDF, "tmp/rawdata.snappy.avro", 1)
    this.saveParquet(rawDF, "tmp/rawdata.snappy.parquet", 1)
    this.saveJson(rawDF, "tmp/rawdata.gzip.json", 1)
  }

  def getExampleDataFrame(exampleDataPath: String): DataFrame = {
    spark.read
      .option("header", "true")
      .csv(exampleDataPath)
      .withColumnRenamed("_c0", "Seq")
  }

  def saveOrc(df: DataFrame, path: String, splitFileCount: Int): Unit = {
    df.coalesce(splitFileCount)
      .write
      .mode(SaveMode.Overwrite)
      .option("compression", "snappy")
      .orc(path)
  }

  def saveAvro(df: DataFrame, path: String, splitFileCount: Int): Unit = {
    df.coalesce(splitFileCount)
      .write
      .mode(SaveMode.Overwrite)
      .option("compression", "snappy")
      .avro(path)
  }

  def saveParquet(df: DataFrame, path: String, splitFileCount: Int): Unit = {
    df.coalesce(splitFileCount)
      .write
      .mode(SaveMode.Overwrite)
      .option("compression", "snappy")
      .parquet(path)
  }

  def saveJson(df: DataFrame, path: String, splitFileCount: Int): Unit = {
    df.coalesce(splitFileCount)
      .write
      .mode(SaveMode.Overwrite)
      .option("compression", "gzip")
      .json(path)
  }
}
