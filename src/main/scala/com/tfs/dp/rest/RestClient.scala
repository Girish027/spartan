package com.tfs.dp.rest

import java.io.{BufferedReader, InputStreamReader, OutputStreamWriter}
import java.net.{HttpURLConnection, URL}

import com.tfs.dp.exceptions.RestClientException
import org.apache.logging.log4j.scala.Logging

/**
  * Created by devinder.k on 12/19/2017.
  *
  * This class currently supports JSON as request Body and JSON as resonse only.
  */

trait RestService {
  def sendPostRequest(requestUrl: String, payload: String): String
  def sendGetRequest(requestUrl: URL): String
}

object RestClient extends RestService with Logging{

  def sendPostRequest(requestUrl: String, payload: String): String = {
    //TODO: To be implemented
    "POST Response"
  }

  def sendGetRequest(requestUrl: URL): String = {
    val jsonString = new StringBuffer
    try {
      val connection:HttpURLConnection = requestUrl.openConnection().asInstanceOf[HttpURLConnection]
      connection.setRequestMethod("GET")
      val responseCode = connection.getResponseCode();
      if(HttpURLConnection.HTTP_OK == responseCode){
        val br = new BufferedReader(new InputStreamReader(connection.getInputStream))
        var line:String = ""
        while ( {
          line = br.readLine()
          null != line
        }) jsonString.append(line)
        br.close()
        connection.disconnect
      }
    } catch {
      case ex: Exception =>
        logger.error(s"Exception while making REST call for url: $requestUrl", ex)
        throw RestClientException(s"Exception while making REST call for url: $requestUrl")
    }
    jsonString.toString
  }

}