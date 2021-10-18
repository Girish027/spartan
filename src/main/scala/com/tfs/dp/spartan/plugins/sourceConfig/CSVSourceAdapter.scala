package com.tfs.dp.spartan.plugins.sourceConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

class CSVSourceAdapter extends SourceAdapterPlugin {


 override def readData(spark: SparkSession, finalPaths: Seq[String],
                        sourceConf: Map[String, String]): DataFrame = {
    var delimiter:String = {
      if(sourceConf.get("delimiter").nonEmpty)
      {sourceConf("delimiter")
      }
      else
        "," //default delimiter: comma
    }
    logger.info(s"Delimiter for CSV reading: " + delimiter.toString)

    spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter",delimiter).load(finalPaths: _*)

  }
}
