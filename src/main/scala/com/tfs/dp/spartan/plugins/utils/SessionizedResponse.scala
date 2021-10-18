package com.tfs.dp.spartan.plugins.utils

object SessionizedResponse {

  case class Response(var uuid: String = "UUID", var logLabel: String = "LOGLABEL", var callStartTime: Long = 0,
                      var endStatusCode: String = "ENDSTATUSCODE", var networkEntryId: String = "NEID",
                      var logMessage: String = "LOGMSG", var dnis: String = "DNIS", var duration: String = "DURATION",
                      var cpn: String = "ANI", var iiDigits: String = "ANI_II", var iiPrivacy: String = "ANI_PRIVATE",
                      var destinationNumber: String = "DESTINATIONNUMBER", var transportModeId: String = "TRANSPORTMODEID",
                      var host: String = "HOST", var wt: String = "TYPE", var entity: String = "ENTITY", var uri: String = "URI",
                      var userdata: String = "USERDATA", var bridged: String = "BRIDGED", var endreason: String = "ENDREASON",
                      var transfer_fail_reason: String = "TRANSFERFAILREASON", var connection_result: String = "CONNECTIONRESULT",
                      var call_type: String = "CALLTYPE", var call_done_reason: String = "CALLDONEREASON",
                      var log_duration: String = "LOGDURATION", var dcr: String = "DCR", var reco_result_nomatch: String = "RESULTNOMATCH",
                      var reco_result_noinput: String = "RESULTNOINPUT", var reco_result_success: String = "RESULTSUCCESS",
                      var error_description: String = "ERROR", var unix_timestamp: String = "UNIXTIMESTAMP",
                      var devlog_size: String = "DEVLOGSIZE", var tellme_session_id: String = "SESSIONID", var key: String = "KEY") extends TransformedResponse

  abstract class TransformedResponse

}