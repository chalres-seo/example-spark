package com.example.apps

import com.example.spark.{DataFrameUtils, SparkFactory}
import com.example.utils.AppConfig
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hive.ql.exec.spark.SparkUtilities
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.annotation.tailrec

object AppMain extends LazyLogging {

  // Init SparkSession.
  private val spark: SparkSession = SparkFactory.getSparkSession

  import spark.implicits._

  private val exampleCsvDataPath: String = "tmp/your-example-data.csv"
  private val exampleOrcDataPath: String =  "tmp/rawdata.orc"
  private val exampleAvroDataPath: String = "tmp/rawdata.avro"

  def main(args: Array[String]): Unit = {
    logger.debug(s"start spark app. name: ${AppConfig.sparkAppName}")
    logger.debug(s"spark version: ${spark.version}")

    this.cleanAndSaveRawData()

    val sourceDF = spark.read.orc(exampleOrcDataPath)

    sourceDF.show

    sourceDF.groupBy($"age", $"clothing_id").count.show
  }

  def cleanAndSaveRawData() = {
    logger.debug("load raw csv data.")
    val df = spark.read.option("header", "true").csv(exampleCsvDataPath)
    logger.debug("raw record count: " + df.count)

    logger.debug("clean up csv data")
    val cleanupDF = df.select($"seq".cast("Int"),
      $"Clothing ID".cast("Int"),
      $"Age".cast("Int"),
      $"Rating".cast("Int"),
      $"Recommended IND".cast("Int"),
      $"Positive Feedback Count".cast("Int"),
      $"Division Name",
      $"Class Name").filter($"seq".isNotNull && $"Rating".isNotNull && $"Division Name".isNotNull && $"Recommended IND".isNotNull)

    val cleanupDFWithRename = DataFrameUtils.replaceAllColumnName(cleanupDF)(_.replace(" ", "_").toLowerCase)
    //renameDF.show()
    logger.debug("clean up record count: " + cleanupDFWithRename.count())

    logger.debug("save clean up data.")
    DataFrameUtils.saveOrc(cleanupDFWithRename.coalesce(1), exampleOrcDataPath)
    DataFrameUtils.saveAvro(cleanupDFWithRename.coalesce(1), exampleAvroDataPath)

  }

  def cleanUpCsv(rdd: RDD[String]): RDD[String] = {
    val base: RDD[(Long, String)] = rdd
      .filter(_ != "")
      .zipWithIndex
      .filter(_._2 > 0)
      .map(s => (s._2, s._1))

    val line = base.filter(_._2(0).isDigit)
    val malformedLine = base.filter(!_._2(0).isDigit).map(s => (s._1 - 1, s._2))

    logger.debug(s"base: ${base.count()}")
    logger.debug(s"line: ${line.count()}")
    logger.debug(s"malformedLine: ${malformedLine.count()}")

    @tailrec
    def loop(currentLine: RDD[(Long, String)], currentMalformedLine: RDD[(Long, String)]): RDD[(Long, String)] = {
      logger.debug(s"currentLine: ${currentLine.count()}")
      logger.debug(s"currentMalformedLine: ${currentMalformedLine.count()}")

      val currentLinePartitioners: HashPartitioner = new HashPartitioner(currentLine.partitions.length)
      val currentMalformedLinePartitioners: HashPartitioner = new HashPartitioner(currentMalformedLine.partitions.length)

      if (currentMalformedLine.isEmpty()) {
        currentLine
      } else {
        val joinedRdd: RDD[(Long, (String, Option[String]))] = currentLine
          .leftOuterJoin(currentMalformedLine, currentLinePartitioners)

        logger.debug(s"joined: ${joinedRdd.count()}")

        val joinedMalform: RDD[(Long, String)] = joinedRdd.filter(_._2._2.isDefined).map(s => (s._1, s._2._2.get))

        logger.debug(s"joinedMalform: ${joinedMalform.count()}")

        val nextLine = joinedRdd.map(s => s._2._2 match {
          case Some(_) => (s._1, s._2._1 + s._2._2.get)
          case None => (s._1, s._2._1)
        })

        val nextMalformedLine = currentMalformedLine.subtractByKey(joinedMalform, currentMalformedLinePartitioners)

        logger.debug(s"nextLine: ${nextLine.count()}")
        logger.debug(s"nextMalformedLine: ${nextMalformedLine.count()}")

        loop(nextLine, nextMalformedLine.map(s => (s._1 - 1, s._2)))
      }
    }

    loop(line, malformedLine).map(_._2)
  }
}
