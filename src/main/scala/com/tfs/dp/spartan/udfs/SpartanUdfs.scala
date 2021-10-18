package com.tfs.dp.spartan.udfs

import com.github.nscala_time.time.Imports.DateTime
import com.tfs.dp.crypto.EncryptionUtil
import com.tfs.dp.spartan.conf.Consts
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by guruprasad.gv on 8/29/17.
  */
object SpartanUdfs {

  def isInRange(startDate: DateTime, endDate: DateTime) = udf((year: Int, month: Int, day: Int, hour: Int, min: Int) => {

    val dateStr: String =
      "%04d".format(year) +
        "%02d".format(month) +
        "%02d".format(day) +
        "%02d".format(hour) +
        "%02d".format(min);

    val partitionDate = Consts.partitionIdFormater.parseDateTime(dateStr)

    (partitionDate.isEqual(startDate) || partitionDate.isAfter(startDate)) &&
      partitionDate.isBefore(endDate)
  })

  def isFieldExists(row:Row, field:String) = {
    import scala.util.{Failure, Success, Try}
    val index: Boolean = Try(row.fieldIndex(field)) match {
      case Success(_) => true
      case Failure(_) => false
    }
    index
  }

  def getFieldValue(row:Row, field:String) = {
    import scala.util.{Failure, Success, Try}
    val value: String = Try(row.fieldIndex(field)) match {
      case Success(_) => row.getString(row.fieldIndex(field))
      case Failure(_) => ""
    }
    value
  }

  def registerAll(sparkSession: SparkSession):Unit = {
    sparkSession.sqlContext.udf.register("isFieldExists", isFieldExists _)
    sparkSession.sqlContext.udf.register("getFieldValue", getFieldValue _)
    sparkSession.sqlContext.udf.register("encrypt", EncryptionUtil.encrypt _)
    sparkSession.sqlContext.udf.register("decrypt", EncryptionUtil.decrypt _)
  }
}