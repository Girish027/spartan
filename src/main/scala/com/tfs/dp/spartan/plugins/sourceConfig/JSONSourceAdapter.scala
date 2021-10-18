package com.tfs.dp.spartan.plugins.sourceConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

class JSONSourceAdapter extends  SourceAdapterPlugin{

  override def readData(spark: SparkSession, finalPaths: Seq[String],
                        sourceConf: Map[String, String]): DataFrame ={

    spark.sqlContext.read.json(finalPaths: _*)
  }

}
