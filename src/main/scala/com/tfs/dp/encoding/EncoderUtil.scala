package com.tfs.dp.encoding

import java.net.{URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.DataFrame

object EncoderUtil extends Logging{

  def encodeColumnNamesInDf(partitionedDateDf: DataFrame) :DataFrame ={
    var encodedDf = partitionedDateDf
    for(col <- partitionedDateDf.columns){
      encodedDf = encodedDf.withColumnRenamed(col, URLEncoder.encode(col, StandardCharsets.UTF_8.toString))
    }
    encodedDf
  }

  def decodeColumnNamesInDf(partitionedDateDf: DataFrame): DataFrame ={
    var decodedDf = partitionedDateDf
    for(col <- partitionedDateDf.columns){
      decodedDf = decodedDf.withColumnRenamed(col, URLDecoder.decode(col, StandardCharsets.UTF_8.toString))
    }
    decodedDf
  }
}
