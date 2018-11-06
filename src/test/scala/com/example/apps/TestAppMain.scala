package com.example.apps

import com.example.spark.SparkFactory
import org.junit.Test

class TestAppMain {
  @Test
  def testAppMain(): Unit = {
    // Init Local SparkSession.
    SparkFactory.getSparkSession("local[*]")

    AppMain.main(Array(""))
  }
}


