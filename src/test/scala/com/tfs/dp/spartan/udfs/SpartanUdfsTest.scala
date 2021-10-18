package com.tfs.dp.spartan.udfs

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec

class SpartanUdfsTest extends FlatSpec with Logging {

  val spark = SparkSession
    .builder()
    .appName("SpartanUdfsTest")
    .master("local[*]")
    .getOrCreate()

  SpartanUdfs.registerAll(spark)

  it should "verify isFieldExists udf" in {

    import spark.implicits._

    val df = spark.read.json("./unitTestData/sampleJsonData/test.json")
    df.createOrReplaceTempView("people")

    var sqlDF = spark.sql("SELECT isFieldExists(cars, 'car1') as result FROM people")
    sqlDF = sqlDF.filter($"result" === false)
    assert(sqlDF.count() === 0)

    sqlDF = spark.sql("SELECT isFieldExists(cars, 'car123') as result FROM people")
    sqlDF = sqlDF.filter($"result" === true)
    assert(sqlDF.count() === 0)
  }

  it should "verify the getFieldValue udf" in {

    import spark.implicits._

    val df = spark.read.json("./unitTestData/sampleJsonData/test.json")
    df.createOrReplaceTempView("people")

    var sqlDF = spark.sql("SELECT getFieldValue(cars, 'car1') as result FROM people")
    sqlDF = sqlDF.filter($"result" === "Ford")
    assert(sqlDF.count() === 1)

    sqlDF = spark.sql("SELECT getFieldValue(cars, 'car123') as result FROM people")
    sqlDF = sqlDF.filter($"result" === "")
    assert(sqlDF.count() === 1)
  }

  it should "verify the Encrypt udf" in {
    import spark.implicits._
    val df = spark.read.json("./unitTestData/sampleJsonData/test.json")
    df.createOrReplaceTempView("people")
    spark.sql("select encrypt(name) as name from people").createOrReplaceTempView("encryptPeople")
    var sqlDF = spark.sql("select decrypt(name) as name from encryptPeople")
    sqlDF = sqlDF.filter($"name" === "John")
    assert(sqlDF.count() === 1)
  }
}
