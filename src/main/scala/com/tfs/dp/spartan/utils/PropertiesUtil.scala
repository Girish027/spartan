package com.tfs.dp.spartan.utils

import com.tfs.dp.spartan.conf.Consts
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.collection.Map

/**
  * Created by devinder.k on 1/23/2018.
  */
trait PropertiesUtil{
  def getProperty(key:String):String
}
object PropertiesUtil extends PropertiesUtil with Logging{

  private var spartanProperties: Map[String,String] = null

  def init(spark: SparkSession): Unit ={
    var configFilePath = System.getProperty(Consts.CONFIG_FILEPATH_KEY)
    try {
      val sc: SparkContext = spark.sparkContext
      logger.info(s"Loading spartan properties from location $configFilePath")
      val propsRDD = sc.textFile(configFilePath)
      spartanProperties = propsRDD.map(line => line.split("=")).map(entry => (entry(0), entry(1))).collectAsMap()
    } catch {
      case ex:Exception =>
        logger.error(s"Error while reading Properties File: ${configFilePath}", ex)
        throw ex
    }
  }

  def getProperty(key:String):String = {
    spartanProperties.get(key).get
  }
}
