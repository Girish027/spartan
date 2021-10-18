package com.tfs.dp.spartan.manager

import java.io.File
import java.net.URI
import java.util.UUID

import com.tfs.dp.encoding.EncoderUtil
import com.tfs.dp.exceptions._
import com.tfs.dp.spartan.dg.DGCatalogModel._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

object CustomOutputPartitioner extends Logging{

  def write(spark: SparkSession, df: DataFrame, outputPath: String, stagingDirectory: String, epochTimeStampColumn: String, client: String, serd: Serde.Value) {
    //TODO: Check for correctness of output path

    // For implicit conversions like converting RDDs to DataFrames
    import org.apache.spark.sql.functions._
    import spark.implicits._
      val isEpochTimeColumnPresent = df.columns.contains(epochTimeStampColumn)
      if (!isEpochTimeColumnPresent) {
        logger.error(s"EpochTime column $epochTimeStampColumn is not present in df, output partition will fail, hence exiting")
        throw CustomOutputPartitionerException(s"EpochTime column $epochTimeStampColumn is not present in df, output partition will fail, hence exiting")
      }
      else
        logger.info(s"EpochTime column $epochTimeStampColumn is present in df, continuing with output partitioning")

      val clientBroadcast = spark.sparkContext.broadcast(client)

    logger.info("starting with partitionedDateDf")
      val partitionedDateDf = EncoderUtil.encodeColumnNamesInDf(df)
        .na.fill("") // Filling all null values with empty string
        .withColumn("date_time_generated", ((new Column(epochTimeStampColumn)).cast("long") / 1000).cast("timestamp")) // cast to timestamp

        .withColumn("year", year($"date_time_generated"))
        .withColumn("month", format_string("%02d", month($"date_time_generated")))
        .withColumn("day", format_string("%02d", dayofmonth($"date_time_generated")))
        .withColumn("hour", format_string("%02d", hour($"date_time_generated")))
        .withColumn("minute", minute($"date_time_generated").cast("int"))
        .withColumn(
          "min",
          when($"minute" >= 0 and $"minute" < 15, format_string("%02d", lit(0)))
            .when($"minute" >= 15 and $"minute" < 30, lit(15))
            .when($"minute" >= 30 and $"minute" < 45, lit(30))
            .otherwise(lit(45)))
        .drop("date_time_generated")
        .drop("minute")
      partitionedDateDf.persist(StorageLevel.MEMORY_AND_DISK)
      val uniqueTempOutputPath: String = stagingDirectory + File.separator + UUID.randomUUID() + File.separator


      logger.info("Directories to be cleaned")
      var dfTemp = partitionedDateDf.select($"year", $"month", $"day", $"hour", $"min").map(r => (
        "/client=" + clientBroadcast.value
          + "/year=" + r.getInt(r.length - 5)
          + "/month=" + r.getString(r.length - 4)
          + "/day=" + r.getString(r.length - 3)
          + "/hour=" + r.getString(r.length - 2)
          + "/min=" + r.getString(r.length - 1)))
    logger.info("dropping duplicates")
      dfTemp = dfTemp.dropDuplicates()
    logger.info("dropped duplicates")

      val cleanUpDirectoryList = dfTemp.collect()
    logger.info("dfTemp collect")

      serd match {
        //save to temp output path, then move the data to actual partitioned locations
        case Serde.JSON =>
          partitionedDateDf.write
            .format("json")
            .partitionBy("client", "year", "month", "day", "hour", "min")
            .mode(SaveMode.Overwrite)
            .save(uniqueTempOutputPath)
        case Serde.CSV =>
          partitionedDateDf.write
            .format("com.databricks.spark.csv")
            .partitionBy("client", "year", "month", "day", "hour", "min")
            .option("header", "true")
            .mode(SaveMode.Overwrite)
            .save(uniqueTempOutputPath)
        case Serde.PARQUET =>
          partitionedDateDf.write
            .format("parquet")
            .partitionBy("client", "year", "month", "day", "hour", "min")
            .mode(SaveMode.Overwrite)
            .save(uniqueTempOutputPath)
        case Serde.ORC =>
          partitionedDateDf.write
            .format("orc")
            .partitionBy("client", "year", "month", "day", "hour", "min")
            .mode(SaveMode.Overwrite)
            .save(uniqueTempOutputPath)
        //backward compatible
        case _ =>
          partitionedDateDf.write
            .format("json")
            .partitionBy("client", "year", "month", "day", "hour", "min")
            .mode(SaveMode.Overwrite)
            .save(uniqueTempOutputPath)
      }

      //copy data from temp location to actual location
      try {
        for (dir <- cleanUpDirectoryList) {
          copyDirectories(uniqueTempOutputPath + dir, outputPath + dir)
        }
      } finally {
        deleteFilesFromDirectory(uniqueTempOutputPath)
      }

      for (dir <- cleanUpDirectoryList) {
        createSuccessFile(outputPath + dir)
      }
      partitionedDateDf.unpersist()
  }




  def createSuccessFile(filePath: String) {
      val uri = URI.create(filePath)
      val conf = new Configuration()
      val fs = FileSystem.get(uri, conf);

      val path = filePath + "/_SUCCESS"
      logger.info("Path: " + path)
      try {
        fs.create(new Path(path), true);
      } catch {
        case e: Exception =>
          logger.error("Unable to write success file",e)
          throw CustomOutputPartitionerException("Unable to write success file", e)
      } finally {
        fs.close()
      }
    }

    def cleanDirectories(filePath: String) {
      val uri = URI.create(filePath)
      val conf = new Configuration()
      val fs = FileSystem.get(uri, conf);
      logger.info(s"Path to delete: ${filePath}")

      if (fs.isDirectory(new Path(filePath))) {
        val listOfFiles = fs.listFiles(new Path(filePath), true)
        while (listOfFiles.hasNext()) {
          val file = listOfFiles.next()
          try {
            fs.delete(file.getPath, true);
          } catch {
            case e: Exception =>
              logger.error("Failed to clean up output directory", e)
              throw CustomOutputPartitionerException("Failed to clean up output directory", e)
          }

        }
      }
      fs.close()
    }

  def isPathExist(path: String): Boolean = {
      val uri = URI.create(path)
      val conf = new Configuration()
      val fs = FileSystem.get(uri, conf);
      fs.exists(new Path(path))

  }
  def isGlobStatus(path: String): Boolean = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val files = fs.globStatus(new Path(path))
    if(files.isEmpty)
      {
        return false
      }
    true
  }



  def deleteFilesFromDirectory(directoryPath:String): Unit ={
    logger.info("Deleting Staging Directory: "+directoryPath)
    val conf = new org.apache.hadoop.conf.Configuration()
    val fs = FileSystem.get(new Path(directoryPath).toUri,conf)
    if(fs.exists(new Path(directoryPath)) && fs.isDirectory(new Path(directoryPath))){
      try{
        fs.delete(new Path(directoryPath), true)
      }catch {
        case e: Exception =>
          logger.error(s"Failed to delete staging directory $directoryPath", e)
          throw CustomOutputPartitionerException("Failed to delete staging directory $directoryPath", e)
      }

    }
    fs.close()
  }

  def copyDirectories(sourcePath: String, destinationPath: String): Unit ={
    logger.info("copying directory src:"+sourcePath+" to dest:"+destinationPath)
    val conf = new org.apache.hadoop.conf.Configuration()

    val src:Path = new org.apache.hadoop.fs.Path(sourcePath)
    val fs = FileSystem.get(src.toUri,conf)

    val srcPath: Path = new Path(sourcePath)
    val srcFs =FileSystem.get(srcPath.toUri,conf)

    val dstPath:Path =new Path(destinationPath)
    val dstFs =FileSystem.get(dstPath.toUri,conf)

    //val exists = fs.exists(new org.apache.hadoop.fs.Path(hdfsDirectory))
    if (fs.isDirectory(new Path(sourcePath))) {
      val listOfFiles = fs.listFiles(new Path(sourcePath), true)
      while (listOfFiles.hasNext()) {
        val file = listOfFiles.next()
        try {
          FileUtil.copy(srcFs, file.getPath, dstFs, new Path(dstPath+ File.separator +file.getPath.getName), false, conf)
        } catch {
          case e: Exception =>
            logger.error(s"Failed to copy file ${file.getPath.toString} to ${dstPath+ File.separator + file.getPath.getName}", e)
            throw CustomOutputPartitionerException("Failed to copy file ${file.getPath.toString} to ${dstPath+ File.separator + file.getPath.getName}", e)
        }
      }
    }

  }

  //Feature to create all nullable columns in a data frame
  def setNullableStateForAllColumns( df: DataFrame, nullable: Boolean) : DataFrame = {
    df.sqlContext.createDataFrame(df.rdd, StructType(df.schema.map(_.copy(nullable = nullable))))
  }

  }
