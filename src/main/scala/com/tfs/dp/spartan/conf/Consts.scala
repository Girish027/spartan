package com.tfs.dp.spartan.conf


import com.github.nscala_time.time.Imports.DateTimeFormat
import com.tfs.dp.spartan.Utils.Granularity


/**
  * Created by guruprasad.gv on 8/29/17.
  */
object Consts {
  val partitionIdFormater = DateTimeFormat.forPattern("yyyyMMddHHmm").withZoneUTC();
  val ENCRYPTION_TYPE = "BOUNCYCASTLE"
  val ENCRYPTION_ALGORITHM = "PBEWITHSHA256AND256BITAES-CBC-BC"
  val ENCRYPTION_PASSWORD = "jasypt"
  val CATALOG_SERVICE_BASE_URL_KEY = "catalog.base.url"
  val CATALOG_GET_VIEW_DEFN_API = "catalog.getViewAPI"
  val CATALOG_GET_DQ_DEFN_API = "catalog.getDQDefAPI"
  val CONFIG_FILEPATH_KEY = "spartan.configuration"
  val SPARTAN_PROPERTIES = "spartan.properties"
  val validMinPartitionValues_15Min = List(0,15,30,45)
  val validMinPartitionValues_Hourly = List(0)
  val defaultRawDataGranularity = Granularity.FIFTEEN_MIN
  val DEBUG_VIEW_LOCATION = "catalog.devMode.views"
  val SCHEMA_LOCATION_KEY = "spartan.schemas.path"
  val TEMP_WRITE_DIRECTORY = "/tmp/dataplatTemp/"
  val KEY_REPLAY_TASK = "replayTask"

  val OOZIE_JOB_ID = "oozie.job.id"

  val ES_NODES = "es.nodes"
  val ES_PORT = "es.port"
  val ES_INDEX_AUTO_CREATE = "es.index.auto.create"
  val ES_NODES_WAN_ONLY = "es.nodes.wan.only"
  val ES_DQ_REPORT_SUMMARY_INDEX = "es.dq.summaryReport.index"
  val ES_DQ_REPORT_SUMMARY_INDEX_TYPE = "es.dq.summaryReport.index.type"
  val ES_DQ_REPORT_DETAILED_INDEX = "es.dq.detailedReport.index"
  val ES_DQ_REPORT_DETAILED_INDEX_TYPE = "es.dq.detailedReport.index.type"
  val DQ_ERROR_RECORDS_LIMIT = "dq.error.records.limit"
  val DQ_SUMMARY_REPORT_URL = "dq.summary.report.url"
  val DQ_DETAIL_REPORT_URL = "dq.detail.report.url"
  val ES_DQ_SUMMARY_MAPPING_ID_KEY = "es.mapping.id"

  val MAIL_SMTP_HOST = "mail.smtp.host"
  val MAIL_SMTP_PORT = "mail.smtp.port"
  val MAIL_FROM_ADDR = "mail.from.addr"
  val MAIL_TO_ADDR = "mail.to.addr"
  val MAIL_SMTP_AUTH = "mail.smtp.auth"
  val MAIL_SMTP_START_TLS_ENABLE = "mail.smtp.starttls.enable"
  val MAIL_DQ_SUBJECT_PREFIX = "DQ FAILED:"

  val DEFAULT_VIEW_DQ_ENABLED = false
  val READ_MATERIALIZATION_ENABLED = false

  val GENERIC_SPARK_SQL_PLUGIN_MAIN_CLASS = "com.tfs.dp.spartan.plugins.sql.GenericSparkSQLPlugin"

  val GENERIC_SOURCE_PLUGIN_MAIN_CLASS = "com.tfs.dp.spartan.plugins.sourceConfig.GenericSourcePlugin"
  val AVRO_SOURCE_ADAPTER_MAIN_CLASS ="com.tfs.dp.spartan.plugins.sourceConfig.AVROSourceAdapter"
  val JSON_SOURCE_ADAPTER_MAIN_CLASS ="com.tfs.dp.spartan.plugins.sourceConfig.JSONSourceAdapter"
  val JSONSeq_SOURCE_ADAPTER_MAIN_CLASS ="com.tfs.dp.spartan.plugins.sourceConfig.JSONSeqSourceAdapter"
  val CSV_SOURCE_ADAPTER_MAIN_CLASS ="com.tfs.dp.spartan.plugins.sourceConfig.CSVSourceAdapter"
  val AssistInteraction_SOURCE_ADAPTER_MAIN_CLASS ="com.tfs.dp.spartan.plugins.sourceConfig.AssistInteractionsSourceAdapter"
  val AssistAgentStats_SOURCE_ADAPTER_MAIN_CLASS ="com.tfs.dp.spartan.plugins.sourceConfig.AssistAgentStatsSourceAdapter"
  val AssistInteractionV2_SOURCE_ADAPTER_MAIN_CLASS ="com.tfs.dp.spartan.plugins.sourceConfig.AssistInteractionsV2SourceAdapter"
  val AssistTranscript_SOURCE_ADAPTER_MAIN_CLASS ="com.tfs.dp.spartan.plugins.sourceConfig.AssistTranscriptSourceAdapter"
  val ActiveShare_SOURCE_ADAPTER_MAIN_CLASS ="com.tfs.dp.spartan.plugins.sourceConfig.ActiveShareSourceAdapter"
  val Sessionized_Binlog_SOURCE_ADAPTER_MAIN_CLASS ="com.tfs.dp.spartan.plugins.sourceConfig.SessionizedBinlogSourceAdapter"

  val DEFAULT_VIEW_MATERIALIZATION_ENABLED = true

  val REST_SOURCE_ADAPTER_TIME_DIMENSION_COLUMN ="timeDimensionColumnName"

  val DEFAULT_LOAD_STATEGY = ViewInputLoadStrategy.RELATIVE
  val INTEGER_LITERAL_TEN = 10
  val SQL_SELECT_ALL = "select *"
  val SEPARATOR_COMMA = ","
  val SPACE = " "
  val ENCRYPT_UDF_NAME = "encrypt"
  val DECRYPT_UDF_NAME="decrypt"
  val OPENING_BRACKET = "("
  val CLOSING_BRACKET = ")"
  val SQL_AS = "as"
  val ENCRYPTED_SUFFIX = "_encrypted"
  val DECRYPTED_SUFFIX = "_decrypted"
  val SQL_FROM = "from"
  val BACK_QUOTE = "`"
  val pattern1 = "(<[y]*?>|<[m]*?>|<[d]*?>|<[ymd:_-]*?>)".r
  val pattern = "\\<.*\\>".r
  val pattern3 = "\\>.*".r

  val CLIENTMETRICS_ESINDEX = "clientMetrics.index"
  val NOMINAL_TIME = "nominalTime"
  val VIEW_NAME = "view"
  val CLIENT_NAME = "client"
  val CLIENTMETRICS_TOPIC = "clientMetrics.topic"
}

object ViewInputLoadStrategy extends Enumeration
{
  type ViewInputLoadStrategy = Value

  val STATIC = Value("STATIC")
  val RELATIVE = Value("RELATIVE")
}
