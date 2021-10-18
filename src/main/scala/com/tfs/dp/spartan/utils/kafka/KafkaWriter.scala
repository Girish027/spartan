package com.tfs.dp.spartan.utils.kafka

/**
 * Created by Priyanka.N on 05-08-2019.
 */

import java.util.Properties

import com.tfs.dp.spartan.utils.PropertiesUtil
import org.apache.kafka.clients.producer.{Producer, KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.logging.log4j.scala.Logging

object KafkaWriter extends Logging {


  var producer:Producer[String,String] = getProducer

  def getProducer = {
    val props: Properties = new Properties();
    props.put("bootstrap.servers", PropertiesUtil.getProperty("kafka.brokers"))
    props.put("retries", Integer.valueOf(1))
    props.put("linger.ms", Integer.valueOf(5000))
    new KafkaProducer[String,String](props,new StringSerializer(),new StringSerializer())
  }

  /**
   *
   * @param topicName
   * @param value
   */
  def pushMetrics(topicName: String, key: String, value: String): Unit = {
    val before = System.currentTimeMillis()

    /* Check for empty topic name */
    if (topicName.isEmpty || topicName.equals(null)) {
      logger.info("Not Pushing Dataplatform metrics to Kafka topic as topic name is not configured")
      return
    }
    logger.info("Writing spartan metrics into Kafka topic " + topicName)

    try {
      producer.send(new ProducerRecord(topicName, key, value))
      logger.info("spartan metrics pushed to kafka")
      logger.info(s"total time taken to push metrics= ${(System.currentTimeMillis() - before)} millis")
    }
    catch {
      case ex: Exception => {
        logger.error("Exception while pushing dataplatform metrics to Kafka topic. ",ex)
        throw new RuntimeException("Unable to push to Kafka.")
      }
    }
  }

  def closeProducer: Unit = {
    producer.flush()
    producer.close()
  }
}
