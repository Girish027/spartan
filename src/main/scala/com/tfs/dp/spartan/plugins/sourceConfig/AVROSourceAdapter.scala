package com.tfs.dp.spartan.plugins.sourceConfig
import org.apache.avro.Schema
import org.apache.spark.sql.{DataFrame, SparkSession}

class AVROSourceAdapter extends SourceAdapterPlugin {



  override def readData(spark: SparkSession, finalPaths: Seq[String],
                        sourceConf: Map[String, String]): DataFrame = {

    val schema = new Schema.Parser().parse(this.getClass.getResourceAsStream("/schema_v18.txt"))
    spark.read.format("com.databricks.spark.avro").option("avroSchema", schema.toString).load(finalPaths: _*)
  }
}
