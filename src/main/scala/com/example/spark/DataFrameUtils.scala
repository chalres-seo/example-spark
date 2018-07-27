package com.example.spark

import org.apache.spark.sql.DataFrame

import scala.annotation.tailrec

object DataFrameUtils {

  def trimColumnName(df: DataFrame): DataFrame = {
    this.replaceAllColumnName(df)(_.replace(" ", ""))
  }

  def replaceAllColumnName(df: DataFrame)(replaceRule: String => String): DataFrame = {
    val columnNameList = df.schema.fields.map(_.name).toVector

    @tailrec
    def loop(df: DataFrame, counter: Int): DataFrame = {
      if (counter < 0) {
        df
      } else {
        val currentColumnName = columnNameList(counter)
        val newColumnName = replaceRule(currentColumnName)
        loop(df.withColumnRenamed(currentColumnName, newColumnName), counter - 1)
      }
    }

    loop(df, columnNameList.length - 1)
  }
}
