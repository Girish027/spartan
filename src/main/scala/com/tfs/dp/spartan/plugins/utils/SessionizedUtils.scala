package com.tfs.dp.spartan.plugins.utils


import com._247.logging.binlog.BinLogEventReader
import com._247.logging.binlog.transformer.tpc.CosmosStreamTransformer
import org.tfs.hadoop.logging.binlog.LogEventWritable

import scala.collection.mutable

object SessionizedUtils
{
  def transformEvent(eventBytes: LogEventWritable): SessionizedResponse.Response= {
    import scala.collection.JavaConversions._

    lazy val xformer = new CosmosStreamTransformer()
    val event1 = eventBytes.getEvent
    val event =   BinLogEventReader.makeLogEventFromBytes(event1.getBuffer.array())
    val events = xformer.transform(event)
    var ut:Float = 0
    var attrMap : mutable.Map[String,String] = mutable.Map()
    for(attr <- events.getAttributeList){
      attrMap.put(attr.getKey,attr.getValue)
      if (attr.getKey.equalsIgnoreCase("log.label")) {
         ut = (attr.getTimestamp)/1000000f
      }
    }

    
    var arg=""
    val resp1 = SessionizedResponse.Response()
    resp1.bridged = attrMap.get("bridged").getOrElse("BRIDGED")
    resp1.uuid = attrMap.get("uuid").getOrElse("UUID")
    resp1.networkEntryId = attrMap.get("network_entry.id").getOrElse("NEID")
    resp1.dnis = attrMap.get("dnis").getOrElse("DNIS")
    resp1.duration = attrMap.get("session.duration").getOrElse("DURATION")
    resp1.cpn = attrMap.get("ani").getOrElse("ANI")
    resp1.iiDigits = attrMap.get("iidigits").getOrElse("ANI_II")
    resp1.iiPrivacy = attrMap.get("iiprivacy").getOrElse("ANI_PRIVATE")
    resp1.destinationNumber = attrMap.get("destination_number").getOrElse("DESTINATIONNUMBER")
    resp1.transportModeId = mode2id(attrMap.get("transport_mode").getOrElse("TRANSPORTMODEID"))
    resp1.host = attrMap.get("server.hostname").getOrElse("HOST")
    resp1.wt = attrMap.get("type").getOrElse("TYPE")
    resp1.entity = attrMap.get("entity").getOrElse("ENTITY")
    resp1.uri = attrMap.get("uri").getOrElse("URI")
    resp1.userdata = attrMap.get("userdata").getOrElse("USERDATA")
    resp1.endreason = attrMap.get("endreason").getOrElse("ENDREASON")
    resp1.transfer_fail_reason = attrMap.get("session.telephony.transfer_fail.reason").getOrElse("TRANSFERFAILREASON")
    resp1.connection_result = attrMap.get("connection.result").getOrElse("CONNECTIONRESULT")
    resp1.call_type = attrMap.get("call.type").getOrElse("")
    resp1.call_done_reason = attrMap.get("session.call_done.reason").getOrElse("")
    resp1.log_duration = attrMap.get("log.time.duration").getOrElse("LOGDURATION")
    resp1.dcr = attrMap.get("dcr").getOrElse("DCR")
    resp1.reco_result_nomatch = attrMap.get("reco_result_nomatch").getOrElse("RESULTNOMATCH")
    resp1.reco_result_noinput = attrMap.get("reco_result_noinput").getOrElse("RESULTNOINPUT")
    resp1.reco_result_success = attrMap.get("reco_result_success").getOrElse("RESULTSUCCESS")
    resp1.error_description = attrMap.get("error.description").getOrElse("ERROR")
    resp1.unix_timestamp = ut.toString
    resp1.devlog_size = attrMap.get("devlog_size").getOrElse("")
    resp1.tellme_session_id = attrMap.get("x-tellme-session-id").getOrElse("SESSIONID")
    resp1.key = attrMap.get("key").getOrElse("KEY")

    var ll = attrMap.get("log.label").getOrElse("LOGLABEL")
    val temp = ll.lastIndexOf(':')
    if (temp > -1) {
      resp1.logLabel = ll.substring(0, temp)
      resp1.logMessage = ll.substring(temp + 1, ll.length)
    } else {
      resp1.logLabel = ll
      resp1.logMessage = ""
    }
    resp1.endStatusCode =  attrMap.get("termination").getOrElse("ENDSTATUSCODE")


    val pattern = "sip:".r    
    resp1.callStartTime =  uuidToTime(resp1.uuid).toLong
    resp1
  }

  def uuidToTime(uuid: String): Double = {
    var tmp = "0x"
    tmp = tmp.concat(uuid.substring(14, 18))
    tmp = tmp.concat(uuid.substring(9, 13))
    tmp = tmp.concat(uuid.substring(0, 8))
    var uuidL = java.lang.Long.decode(tmp).longValue()
    uuidL -= 122192928000000000L
    uuidL &= -1152921504606846977L
    var uuidTime = (uuidL / 1.0E4D).asInstanceOf[Double]
    return uuidTime
  }

  def mode2id(mode :String): String = {
    mode match {
      case "tdm.nms" => return "1"
      case "voip.dc" => return "2"
      case "gateway" => return "3"
      case "socket.tellme" => return "4"
      case "unknown" => return "5"
      case "voip.tdm" => return "6"
      case _: String => return ""
    }
  }
}
