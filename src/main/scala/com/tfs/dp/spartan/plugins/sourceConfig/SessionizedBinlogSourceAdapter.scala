package com.tfs.dp.spartan.plugins.sourceConfig

import com.tfs.dp.spartan.plugins.utils.SessionizedUtils
import org.apache.hadoop.io.Text
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.tfs.hadoop.logging.binlog.LogEventWritable

class SessionizedBinlogSourceAdapter extends SourceAdapterPlugin {


  override def readData(spark: SparkSession, finalPaths:Seq[String],
                        sourceConf: Map[String, String]): DataFrame = {
    val finalPath = finalPaths.mkString(",")

    val SessionizedRawData = spark.sparkContext.sequenceFile(finalPath, classOf[Text], classOf[LogEventWritable]).map(l => l._2).map(l => SessionizedUtils.transformEvent(l))
    var SessionizedRDDwithColumns = SessionizedRawData.map(x =>(x))

    val SessionizedDF = spark.createDataFrame(SessionizedRDDwithColumns).toDF("uuid", "logLabel", "callStartTime", "endStatusCode", "networkEntryId", "logMessage", "dnis", "duration", "cpn", "iiDigits", "iiPrivacy", "destinationNumber", "transportModeId", "host", "type", "entity", "uri", "userdata", "bridged", "endreason", "transfer_fail_reason", "connection_result", "call_type", "call_done_reason", "log_duration", "dcr", "reco_result_nomatch", "reco_result_noinput", "reco_result_success", "error_description", "unix_timestamp", "devlog_size", "tellme_session_id", "key")
  SessionizedDF
  }
}
