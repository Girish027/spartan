package com.tfs.dp.spartan.utils

import com.tfs.dp.crypto.EncryptionUtil
import com.tfs.dp.spartan.dg.DGCatalogModel.TableColumn
import com.tfs.dp.spartan.udfs.SpartanUdfs
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec

/**
  * Created by vijaykumar.m on Jan, 2019
  */
class EncryptionUtilTest extends FlatSpec with Logging {

  val spark = SparkSession
    .builder()
    .appName("SpartanUdfsTest")
    .master("local[*]")
    .getOrCreate()

  SpartanUdfs.registerAll(spark)

  it should "encrypt and decrypt the columns" in {
    import spark.implicits._
    val df = spark.read.option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ").json("./unitTestData/sampleJsonData/test.json")
    df.createOrReplaceTempView("people")

    val tableCols = Seq(TableColumn("name", true), TableColumn("age", false), TableColumn("cars.car1", false),
      TableColumn("cars.car2", false), TableColumn("cars.car3", false))
    val encryptedDf = EncryptionUtil.encryptCols(spark, df, tableCols)
    assert(encryptedDf.filter($"name" === "John").count()===0)
    val decryptedDf = EncryptionUtil.decryptCols(spark, encryptedDf, tableCols)
    assert(decryptedDf.filter($"name" === "John").count()===1)
  }
}
