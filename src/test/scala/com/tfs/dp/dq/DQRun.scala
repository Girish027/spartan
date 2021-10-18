package com.tfs.dp.dq

import com.tfs.dp.spartan.Spartan
import com.tfs.dp.spartan.dg.DGCatalogModel.{ImportStatic, SQL, Serde}
import com.tfs.dp.spartan.dg._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.lit

object DQRun {

  val spartan: Spartan = new Spartan("/")

  def main(args: Array[String]): Unit = {
    val totalStart: Long = System.currentTimeMillis()
    val report: String = "PageLoad"
    val hour: List[String] = "00".split(",").toList
    val esType: String = "demos-20170701"
    val day: String = "20170701"
    val clientList: List[String] = "cap1enterprise".split(",").toList
    addToCatalog()
    registerDQRules(report)
    runDQ(clientList, day, hour, report, esType)
  }

  def addToCatalog(): Unit = {
    DGCatalogRegistry.add("PageLoad","/views/PageLoad",Serde.AVRO,
      Seq(new ImportStatic("RawIDM",null,null)),
      Seq(SQL("PageLoad","""
                    SELECT header.*
                                ,body.typeSpecificBody.body.WebPageLoadEvent.* ,body.typeSpecificBody.body.TargetDeterminationEvent.*
                    from RawIDM

                   """))
      ,Map.empty,
      Map.empty

    )

    DGCatalogRegistry.add("WebPageLoadEvent","/views/WebPageLoadEvent",Serde.AVRO,
      Seq(new ImportStatic("RawIDM",null,null)),
      Seq(SQL("WebPageLoadEvent","""
                    SELECT header.*
                                ,body.typeSpecificBody.body.WebPageLoadEvent.*
                    from RawIDM
                    where  body.typeSpecificBody.body.WebPageLoadEvent IS NOT NULL
                   """))
      ,Map.empty,
      Map.empty
    )
  }

  def runDQ(clientList: List[String], day: String, hour: List[String], report: String, esType: String): Unit = {

    for (client <- clientList){
      println("Running DQ for client: ", client)
      for (hr <- hour){
        val srTable = spartan.use(report, client, day+hr+"00", day+hr+"00")
        println("Before running DQ for client: " + client)
        val dqResult = srTable.dqRunner().runDQ()
        println("After running DQ for client: " + client)
        dqResult.createOrReplaceTempView("dqStatus")
        // val x = sqlContext.sql("create or replace temp view t as select client,testDataSet, GROUP_CONCAT(distinct(dqPass)) AS dqResult from dqStatus where dqPass like ('false') group by client, testDataSet")
        val x = spartan.spark.sqlContext.sql("create or replace temp view t as select client,testDataSet, (count(distinct(dqPass))==1 and max(dqPass) like ('true')) AS dqResult from dqStatus group by client, testDataSet")
        println("After creating temp view for client:" + client)
        val dfs = spartan.spark.sqlContext.sql("select * from t")
        val newDf = dfs.withColumn("hr", lit(hr))
        newDf.show()
      }
    }
  }

  def registerDQRules(report: String):Unit = {

    val dataSet: String = "PageLoad"
    val primaryKey: String = "channel"

    val col1: String = "clientId"
    val rule1: String = "clientId is not NULL"
    val desc1: String = "clientId should be not null"

    val col2: String = "channel"
    val rule2: String = """channel IN ("ONLINE","WEB")"""
    val desc2: String = "Channel value should be among Online or Web!"

    val col3: String = "associativeTag"
    val rule3: String =   """associativeTag REGEXP '[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]-[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]-[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]-[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]-[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]'"""
    val desc3: String = "sdfsd"

    val q1: String = s"""concat($col1, "||", $rule1, "||$desc1") as A"""
    val q2: String = s"""concat($col2, "||", $rule2, "||$desc2") as B"""
    val q3: String = s"""concat($col3, "||", $rule3, "||$desc3") as C"""

    val query: String = s"select $primaryKey, $q1, $q2, $q3 from $dataSet"

    val obj = new DGQualityDefinition(
      TableDQ("PageLoadDQ",
        Seq(
          TestDataSet("PageLoad",InputReader(Seq(new ImportStatic(report, null, null)), query),
            Seq(
              Test("ClientIdNotNull","Client Id cannot be null.", (r1:Row) => {Result(r1.getAs[String]("A").split("\\|\\|")(1) == "true", "ClientId cannot be null")}),
              Test("ApplicationId not null","ApplicationId should be not null", (r1:Row) => {Result(r1.getAs[String]("B").split("\\|\\|")(1) == "true", "ApplicationId should be not null")})
            )
          )
        ),
        Seq()
      )
    )
    DGQualityCatalog.addTableDQ(report, obj)
  }
}
