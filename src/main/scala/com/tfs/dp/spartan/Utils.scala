package com.tfs.dp.spartan

import java.text.SimpleDateFormat
import java.util.Calendar

import com.tfs.dp.spartan.conf.Consts
import com.tfs.dp.spartan.manager.CustomOutputPartitioner
import com.tfs.dp.spartan.utils.PropertiesUtil
import org.apache.logging.log4j.ThreadContext
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime

/**
  * Created by Srinidhi.bs on 11/26/17.
  */
object Utils extends Logging {

  object Granularity extends Enumeration {
    val HOUR = Value("HOUR")
    val FIFTEEN_MIN = Value("15_MIN")
    val DAY = Value("DAY")
  }

  def updateDQStatusInSummaryIndex(spark: SparkSession, status: String): Unit = {
    import org.elasticsearch.spark.sql._
    import spark.implicits._
    val statusDF = Seq((ThreadContext.get("corelatId"), status)).toDF("JobId", "DQStatus")
    logger.info(s"saving to es: ${PropertiesUtil.getProperty(Consts.ES_DQ_REPORT_SUMMARY_INDEX)}/${PropertiesUtil.getProperty(Consts.ES_DQ_REPORT_SUMMARY_INDEX_TYPE)}" +
      (Consts.ES_DQ_SUMMARY_MAPPING_ID_KEY -> "JobId"))
    statusDF.saveToEs(s"${PropertiesUtil.getProperty(Consts.ES_DQ_REPORT_SUMMARY_INDEX)}/${
      PropertiesUtil.getProperty(Consts.ES_DQ_REPORT_SUMMARY_INDEX_TYPE)
    }", getESProps + (Consts.ES_DQ_SUMMARY_MAPPING_ID_KEY -> "JobId"))
  }

  def getESProps: Map[String, String] = {
    Map(Consts.ES_NODES -> PropertiesUtil.getProperty(Consts.ES_NODES),
      Consts.ES_PORT -> PropertiesUtil.getProperty(Consts.ES_PORT),
      Consts.ES_INDEX_AUTO_CREATE -> PropertiesUtil.getProperty(Consts.ES_INDEX_AUTO_CREATE),
      Consts.ES_NODES_WAN_ONLY -> PropertiesUtil.getProperty(Consts.ES_NODES_WAN_ONLY))
  }


  def prepareDynamicBucketPath(rootPath: String, dynamicPath:String, startCalendar:Calendar, endCalendar:Calendar ): String = {

    var datePattern = Consts.pattern.findFirstIn(dynamicPath).get
    datePattern = datePattern.substring(1, datePattern.size-1)
    datePattern = datePattern.replaceFirst("yyyy", getYear(startCalendar))
    datePattern = datePattern.replaceFirst("mm", getMonth(startCalendar))
    datePattern = datePattern.replaceFirst("dd", getDate(startCalendar))
    var suffix = Consts.pattern3.findFirstIn(dynamicPath).get
    suffix = suffix.substring(1,suffix.size)
    var startPath = rootPath + datePattern+suffix
    startPath
  }

  def getFinalPathAsList(rootPath: String, startEpoch: String,
                         endEpoch: String,
                         granularity: Granularity.Value, dynamicBucketPath: Option[String]): Seq[String] = {
    val startSdf = new SimpleDateFormat("yyyyMMddHHmm")
    val startDate = startSdf.parse(startEpoch)
    var startCalendar = startSdf.getCalendar

    val endSdf = new SimpleDateFormat("yyyyMMddHHmm")
    val endDate = endSdf.parse(endEpoch)
    var endCalendar = endSdf.getCalendar
    var finalPaths = Seq[String]()
    var startPath = rootPath
    var dynamicPath: String = dynamicBucketPath.get
    var nextPath: String = ""
    var incrementValue = 60 //for hour
    var timeDiff = (endCalendar.getTimeInMillis - startCalendar.getTimeInMillis) / (1000 * 60) //time diff in mins

    if (dynamicBucketPath.get.isEmpty) {
      if (granularity == Granularity.FIFTEEN_MIN) {
        incrementValue = 15
      }
       startPath = rootPath + "/year=" + getYear(startCalendar) + "/month=" + getMonth(startCalendar) + "/day=" +
        getDate(startCalendar) + "/hour=" + getHour(startCalendar) + "/min=" + getMinute(startCalendar, granularity)
    }
    else {
      incrementValue=1
      timeDiff = (endCalendar.getTimeInMillis - startCalendar.getTimeInMillis) / (1000 * 3600 * 24)
      startPath = prepareDynamicBucketPath(rootPath, dynamicPath, startCalendar, endCalendar )
    }

    if(isValidPath(startPath)) {
      finalPaths = finalPaths :+ startPath
    }

    while (timeDiff > incrementValue) {
      if (incrementValue == 15)
        startCalendar.add(Calendar.MINUTE, 15) // adds 15 min
      else if (incrementValue == 60)
        startCalendar.add(Calendar.HOUR_OF_DAY, 1) // adds 1 hour
      else
        startCalendar.add(Calendar.DAY_OF_MONTH, 1) //adds 1 day

      if (dynamicBucketPath.get.isEmpty) {
        nextPath = rootPath + "/year=" + getYear(startCalendar) + "/month=" + getMonth(startCalendar) + "/day=" +
          getDate(startCalendar) + "/hour=" + getHour(startCalendar) + "/min=" + getMinute(startCalendar, granularity)
        timeDiff = (endCalendar.getTimeInMillis - startCalendar.getTimeInMillis) / (1000 * 60)
      }
      else {
        dynamicPath = dynamicBucketPath.get
        nextPath = prepareDynamicBucketPath(rootPath, dynamicPath, startCalendar, endCalendar)
        timeDiff = (endCalendar.getTimeInMillis - startCalendar.getTimeInMillis) / (1000 * 3600 * 24)
      }
      if (isValidPath(nextPath)) {
        finalPaths = finalPaths :+ nextPath
      }
    }
    logger.info(s"Final Data source path prepared for rootpath: $rootPath startEpoch:" +
      s" $startEpoch endEpoch: $endEpoch granularity: $granularity is: $finalPaths")

    finalPaths
  }

  /*  def isDynamicDatePresent(physicalLocation: Option[String]): Boolean = {
      if (Consts.pattern1.findAllIn(physicalLocation).isEmpty) {
        return false
      }
      return true
    }*/

  def getFinalPathsAsString(rootPath: String, startEpoch: String,
                            endEpoch: String,
                            granularity: Granularity.Value): String = {
    getFinalPathAsList(rootPath, startEpoch, endEpoch, granularity, Some("")).mkString(",")
  }

  def getOutputFinalPath(rootPath: String, startEpoch: String,
                         endEpoch: String,
                         granularity: Granularity.Value): String = {
    val startSdf = new SimpleDateFormat("yyyyMMddHHmm")
    val startDate = startSdf.parse(startEpoch)
    var startCalendar = startSdf.getCalendar

    val endSdf = new SimpleDateFormat("yyyyMMddHHmm")
    val endDate = endSdf.parse(endEpoch)
    var endCalendar = endSdf.getCalendar

    var finalPath = rootPath + "/year=" + getYear(startCalendar) + "/month=" + getMonth(startCalendar) + "/day=" +
      getDate(startCalendar) + "/hour=" + getHour(startCalendar) + "/min=" + getMinute(startCalendar, granularity)

    if (!CustomOutputPartitioner.isPathExist(finalPath)) {
      finalPath = ""
    }

    var incrementValue = 60 // for hour
    if (granularity == Granularity.FIFTEEN_MIN)
      incrementValue = 15

    var timeDiffInMin = (endCalendar.getTimeInMillis - startCalendar.getTimeInMillis)/(1000*60)

    while (timeDiffInMin > incrementValue) {
      if (granularity == Granularity.FIFTEEN_MIN)
        startCalendar.add(Calendar.MINUTE, 15) // adds 15 min
      else
        startCalendar.add(Calendar.HOUR_OF_DAY, 1) // adds 1 hour

      timeDiffInMin = (endCalendar.getTimeInMillis - startCalendar.getTimeInMillis)/(1000*60)
      val nextPath = rootPath + "/year=" + getYear(startCalendar) + "/month=" + getMonth(startCalendar) + "/day=" +
        getDate(startCalendar) + "/hour=" + getHour(startCalendar) + "/min=" + getMinute(startCalendar, granularity)

      if (CustomOutputPartitioner.isPathExist(nextPath)) {
        if ("".equals(finalPath))
          finalPath = nextPath
        else
          finalPath += "," + nextPath
      }
    }
    logger.info(s"Final Destination path prepared for rootpath: $rootPath startEpoch:" +
      s" $startEpoch endEpoch: $endEpoch granularity: $granularity is: $finalPath")

    finalPath
  }

  def getYear(cal: Calendar): String = {
    "%04d".format(cal.get(Calendar.YEAR))
  }

  def getMonth(cal: Calendar): String = {
    "%02d".format(cal.get(Calendar.MONTH) + 1)
  }

  def getDate(cal: Calendar): String = {
    "%02d".format(cal.get(Calendar.DATE))
  }

  def getHour(cal: Calendar): String = {
    "%02d".format(cal.get(Calendar.HOUR_OF_DAY))
  }

  def getMinute(cal: Calendar, granularity: Granularity.Value): String = {
    if (granularity == Granularity.FIFTEEN_MIN)
      "%02d".format(cal.get(Calendar.MINUTE))
    else
      "*"
  }

  def checkValidMinuteValueForRawDataPartition(dateTime: DateTime): Boolean = {
    if (Granularity.HOUR == Consts.defaultRawDataGranularity) {
      Consts.validMinPartitionValues_Hourly.contains(dateTime.getMinuteOfHour)
    } else {
      Consts.validMinPartitionValues_15Min.contains(dateTime.getMinuteOfHour)
    }
  }

def isValidPath(startPath: String):Boolean= {
    if (!startPath.contains("*")) {
      if (CustomOutputPartitioner.isPathExist(startPath)) {
        return true
      } else {
        return false;
      }
    }
    else {
      if (CustomOutputPartitioner.isGlobStatus(startPath)) {
        return true
      }
      else {
        return false;
      }
    }
  }
}
