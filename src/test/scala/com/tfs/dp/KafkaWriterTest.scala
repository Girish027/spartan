package com.tfs.dp

import java.util.concurrent.Future

import com.tfs.dp.spartan.conf.Consts
import com.tfs.dp.spartan.utils.PropertiesUtil
import com.tfs.dp.spartan.utils.kafka.KafkaWriter
import org.apache.kafka.clients.producer.{MockProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec

/**
 * Created by Priyanka.N on 12/08/2019.
 */
class KafkaWriterTest extends FlatSpec{

  "KafkaProducer" should "be sending metrics" in
  {
    val producer =  new MockProducer(true,new StringSerializer,new StringSerializer)
    val corelationId = "12245"
    val statsString = "{\"id\":\"0000137-190812043028730-oozie-oozi-W\",\"eventSource\":\"dp2-jobmetrics\",\"eventTime\":1565681104071,\"body\":{\"viewName_keyword\":\"Digital_Test28\",\"clientName_keyword\":\"hilton\",\"nominal_time\":1541934000000,\"processingExecution_time\":\"1565681104072\",\"dataPaths\":[\"hdfs://nameserviceQAHDP///raw/prod/rtdp/idm/events/hilton/year=2018/month=11/day=11/hour=11/min=00\"]}}"
    val topicName= "topic1"
    var expectedRecord = new ProducerRecord[String, String](topicName, corelationId,statsString)
    val spark = SparkSession.builder.appName("SpartanAppTest")
      .config("spark.sql.crossJoin.enabled", "true")
      .master("local")
      .enableHiveSupport()
        .getOrCreate()
    System.setProperty(Consts.CONFIG_FILEPATH_KEY,"src/test/resources/properties")
    PropertiesUtil.init(spark)
    KafkaWriter.producer=producer
    KafkaWriter.pushMetrics(topicName,corelationId,statsString)

    val metadata:Future[RecordMetadata] = producer.send(expectedRecord)
    assert(metadata.isDone() equals true,"Send should be immediately complete")
    assert(metadata.get().topic() == topicName, "topic name mismatch");
    assert(producer.history().get(0).value() equals expectedRecord.value().toString,"producer history and expected record mismatched")

  }

}
