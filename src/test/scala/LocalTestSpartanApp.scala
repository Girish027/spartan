import com.github.nscala_time.time.Imports._
import com.tfs.dp.spartan.Spartan
import com.tfs.dp.spartan.dg.DGCatalogModel.ImportStatic
import com.tfs.dp.spartan.dg._
import com.tfs.dp.spartan.manager.TableManager
import org.apache.spark.sql.Row

/**
  * Created by guruprasad.gv on 8/26/17.
  */
object LocalTestSpartanApp extends Serializable {

  def main(args: Array[String]): Unit = {


    val spartan: Spartan = new Spartan("/Users/guruprasad.gv/sdata")
    import spartan.spark.implicits._
    val table: TableManager = spartan.use("ResolutionReport", "ebay-small", "201707110000", "201707150000")

    val df = spartan.spark.sql("select channelSessionId, count(*) as cnt from ResolutionReport group by channelSessionId")


    DGQualityCatalog.addTableDQ("ResolutionReport", new DGQualityDefinition(

      TableDQ("ResolutionReport", Seq(
        TestDataSet("EachRowDS", InputReader(Seq(new ImportStatic("ResolutionReport", "201704080100", "201704080500")), "select * from ResolutionReport"),
          Seq(

            Test("WebEventTest", "t1 desc", (r1: Row) => {
              Result(
                r1.getAs[String]("eventType").equalsIgnoreCase("WebEvent"),
                s"Expecting eventType to be WebEvent, found : ${r1.getAs[String]("eventType")}")
            }),
            Test("LinkPropertyEventTest", "t2 desc", (r1: Row) => {
              Result(
                r1.getAs[String]("eventType").equalsIgnoreCase("LinkPropertyEvent"),
                s"Expecting eventType to be LinkPropertyEvent, found : ${r1.getAs[String]("eventType")}")
            })
          )
        ),

        TestDataSet("ChannelSessionCountDS", InputReader(Seq(new ImportStatic("ResolutionReport", "201704080100", "201704080500")), "select channelSessionId, count(*) as cnt from ResolutionReport group by channelSessionId"),
          Seq(

            Test("CountTest", "count testing", (r1: Row) => {
              Result(
                r1.getAs[Long]("cnt") > 1000,
                s"Expecting count to be greater than 1000 : ${r1.getAs[Long]("cnt")}")
            })
          )
        )
      ),
        Seq(
          MonitoredMetric(
            "channelSessionId_counts",
            InputReader(Seq(new ImportStatic("ResolutionReport","201704080100", "201704080500")), "select channelSessionId, count(*) as cnt from ResolutionReport group by channelSessionId"),
            Seq("channelSessionId"),
            Seq("cnt")
          ),
          MonitoredMetric(
            "eventType_counts",
            InputReader(Seq(new ImportStatic("ResolutionReport", "201704080100", "201704080500")), "select eventType, count(*) as cnt from ResolutionReport group by eventType"),
            Seq("eventType"),
            Seq("cnt")
          )
        ))))
    val resultSet = table.dqRunner().runDQ();
    //resultSet.filter(!($"result.dqPass")).filter($"testDataSet" === "ds1").show(10, false)
    //resultSet.filter(!($"result.dqPass")).filter($"testDataSet" === "ds2").show(10, false)
    resultSet.filter(!($"result.dqPass") && ($"testDataSet" === "ds2")).show(10, false)
    resultSet.filter(!($"result.dqPass") && ($"testDataSet" === "ds1")).show(10, false)

    //  df.printSchema();
    //   df.show(20, false)
  }
}
