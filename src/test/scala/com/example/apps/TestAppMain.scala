package com.example.apps

import com.example.spark.SparkFactory
import org.junit.Test

class TestAppMain {
  @Test
  def testAppMain(): Unit = {
    // Init Local SparkSession.
    val spark = SparkFactory.getSparkSession("local[8]")

    AppMain.main(Array(""))
  }
}

