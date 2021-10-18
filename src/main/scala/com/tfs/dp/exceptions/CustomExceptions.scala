package com.tfs.dp.exceptions

/**
  * Created by devinder.k on 1/17/2018.
  */
case class RestClientException(message:String) extends Exception(message)

case class SparkSessionException(message:String) extends Exception(message)

case class DedupException(message:String) extends Exception(message)

case class DataReadException(message:String) extends Exception(message)

case class JsonParseException(message:String) extends Exception(message)

case class CustomOutputPartitionerException(s:String, cause: Throwable= null) extends Exception(s,cause)

case class DQRunException(message:String) extends Exception(message)

case class GenericCatalogException(message:String) extends Exception(message)

case class EmailServiceException(message:String) extends Exception(message)

case class DQExportException(message:String) extends Exception(message)

case class NoDQDefinedException(message:String) extends Exception(message)

case class SpartanPluginException(message:String) extends Exception(message)