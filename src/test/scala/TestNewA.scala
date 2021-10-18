import com.databricks.spark.avro._
import com.github.nscala_time.time.Imports.{DateTime, DateTimeFormat}
import com.tfs.dp.spartan.udfs.SpartanUdfs
import org.apache.spark.sql.SparkSession

/**
  * Created by guruprasad.gv on 8/26/17.
  */
object TestNewA {


  def main(args: Array[String]): Unit = {
    val partitionIdFormater = DateTimeFormat.forPattern("yyyyMMddHHmm");
    val startDate = DateTime.parse("201707110000", partitionIdFormater);
    val endDate = DateTime.parse("201707122300", partitionIdFormater);



    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

    import spark.implicits._
    // isInRange _
    //spark.udf.register("isInRange", isInRange);
    //val partsUdf = udf(isInRange)

    var x = spark.read.avro("/Users/guruprasad.gv/sdata/raw/prod/rtdp/idm/events/eBay");
    //"year=2017/month=07/day=10/hour=*/min=*/packed*.avro")
    x = x.filter(SpartanUdfs.isInRange(startDate, endDate)($"year", $"month", $"day", $"hour", $"min"))
    // x.createOrReplaceTempView("t")
    // x = spark.sql("select *, concat(day,' ',month) as d1 from t")
    //val x = spark.read.json("/Users/guruprasad.gv/Documents/WorkDocs/ex-ss")
    println("asd " + x)
    // x = x.withColumn("date", x("day"))
    //x.printSchema()

    // x.printSchema()
    val st = 15
    val qry: String
    = "day between 15 - 3 and 15 "
    //"date > 20170712"
    //println("count : " + x.where(s"""$qry""").count())

    x.show(1, false)
    println(x.count())
  }
}
