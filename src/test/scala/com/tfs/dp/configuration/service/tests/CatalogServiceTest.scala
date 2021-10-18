package com.tfs.dp.configuration.service.tests

import java.net.URL

import com.tfs.dp.configuration.service.CatalogService
import com.tfs.dp.configuration.service.impl.CatalogServiceImpl
import com.tfs.dp.exceptions.{GenericCatalogException, JsonParseException}
import com.tfs.dp.rest.RestService
import com.tfs.dp.spartan.conf.Consts
import com.tfs.dp.spartan.dg.DGCatalogModel._
import com.tfs.dp.spartan.utils.PropertiesUtil
import net.liftweb.json.JsonParser
import org.scalamock.scalatest.MockFactory
import org.scalatest.FlatSpec

class CatalogServiceTest extends FlatSpec with MockFactory{

//test for Defualt values for DqEnabled = false, loadstrategy = RELATIVE , MaterializatioEnabled = true
  "CatalogService" should "load rawIdm view definition with default values for DqEnabled and MaterializatioEnabled" in {
    val restClientMock = mock[RestService]
    val propertiesUtilMock = mock[PropertiesUtil]
    val catalogService: CatalogService = new CatalogServiceImpl{
      override def restClient = restClientMock
      override def propertiesUtil = propertiesUtilMock
    }
    val expectedPt: PhysicalTable = PhysicalTable("test",
      Location("/reports/test1/%s/", "startTime"),
      Serde.JSON,
      Map("ebay" -> "eBay"),
      Map(),
      None,
      Option(TableBuilder(
        Seq(ImportStatic(PhysicalTable("rawIdm", Location("/raw/prod/rtdp/idm/events/%s/", null), Serde.AVRO, Map("ebay" -> "eBay")),"201704080100", "201704080500")),
        SQLTableBuilderPlugInConf(SpartanPluginTypes.SQL_PROCESSOR.toString, Consts.GENERIC_SPARK_SQL_PLUGIN_MAIN_CLASS, Seq(SQL("test","select header.*, body.typeSpecificBody.body.Treatment.*, body.typeSpecificBody.body.Treatment.deflection.*, body.typeSpecificBody.body.Treatment.reportDimensions.* from rawIdm where body.typeSpecificBody.body.Treatment is not null")))
      )),null,None,Some(Consts.DEFAULT_VIEW_MATERIALIZATION_ENABLED),Consts.DEFAULT_LOAD_STATEGY,Some(Consts.DEFAULT_VIEW_DQ_ENABLED))

    (propertiesUtilMock.getProperty _).expects(Consts.CATALOG_SERVICE_BASE_URL_KEY).returning("http://host298.assist.pool.sv2.247-inc.net:8080/catalog").twice()
    (propertiesUtilMock.getProperty _).expects(Consts.CATALOG_GET_VIEW_DEFN_API).returning("/processor/snapshot").twice()
    (propertiesUtilMock.getProperty _).expects(Consts.DEBUG_VIEW_LOCATION).returning("false").twice()

    val testViewUrl = new URL("http://host298.assist.pool.sv2.247-inc.net:8080/catalog/processor/snapshot?viewname=test&clientname=ebay&scheduleStartTime=1491613200000&scheduleEndTime=1491627600000&finalview=test")
    val rawIdmUrl = new URL("http://host298.assist.pool.sv2.247-inc.net:8080/catalog/processor/snapshot?viewname=rawIdm&clientname=ebay&scheduleStartTime=1491613200000&scheduleEndTime=1491627600000&finalview=test")
    val testViewResponse = "{\n  \"viewame\" : \"test\",\n  \"path\" : \"/reports/test1/%s/\",\n  \"format\" : \"JSON\",\n " +
      "\"timeDimensionColumn\" : \"startTime\",\n " +
      "\"importsList\" : [{\"viewName\":\"rawIdm\",\"startTime\":\"1491613200000\",\"endTime\":\"1491627600000\",\"timeFormat\":\"yyyy-MM-dd HH:mm\"}],\n " +
      "\"clientOverrideMap\":{\"ebay\":\"eBay\"},\n  " +
      "\"sqls\" : [\n    {\"test\" : \"select header.*, body.typeSpecificBody.body.Treatment.*, body.typeSpecificBody.body.Treatment.deflection.*, body.typeSpecificBody.body.Treatment.reportDimensions.* from rawIdm where body.typeSpecificBody.body.Treatment is not null\"}\n  ]\n}"
   val rawIdmResponse = "{\n  \"viewName\" : \"rawIdm\",\n  \"path\" : \"/raw/prod/rtdp/idm/events/%s/\",\n  \"format\" : \"AVRO\",\n  \"clientOverrideMap\":{\"ebay\":\"eBay\"}\n}"

    (restClientMock.sendGetRequest _).expects(testViewUrl).returning(testViewResponse)
    (restClientMock.sendGetRequest _).expects(rawIdmUrl).returning(rawIdmResponse)
    val actualPt: PhysicalTable = catalogService.getViewDefinition("test","ebay","1491613200000","1491627600000","test")
    assert(expectedPt.inputDataLoadStrategy ==actualPt.inputDataLoadStrategy)
    assert(expectedPt.dqEnable ==actualPt.dqEnable)
    assert(expectedPt.materializationEnabled == actualPt.materializationEnabled)
  }


  it should "throw JsonParseException if view doesn't exists" in {
    val restClientMock = mock[RestService]
    val propertiesUtilMock = mock[PropertiesUtil]
    val catalogService: CatalogService = new CatalogServiceImpl{
      override def restClient = restClientMock
      override def propertiesUtil = propertiesUtilMock
    }
    (propertiesUtilMock.getProperty _).expects(Consts.CATALOG_SERVICE_BASE_URL_KEY).returning("http://host298.assist.pool.sv2.247-inc.net:8080/catalog")
    (propertiesUtilMock.getProperty _).expects(Consts.CATALOG_GET_VIEW_DEFN_API).returning("/processor/snapshot")
    (propertiesUtilMock.getProperty _).expects(Consts.DEBUG_VIEW_LOCATION).returning("false")
    val dummyViewUrl = new URL("http://host298.assist.pool.sv2.247-inc.net:8080/catalog/processor/snapshot?viewname=someDummyValue&clientname=ebay&scheduleStartTime=1499817600000&scheduleEndTime=1499817600000&finalview=someDummyValue")
    (restClientMock.sendGetRequest _).expects(dummyViewUrl).returning("{\"viewName\":null,\"path\":null,\"format\":null,\"referenceTime\":null,\"timeDimensionColumn\":null,\"uniqueFields\":[],\"importsList\":[],\"sqls\":[]}")

    assertThrows[JsonParseException] {
      catalogService.getViewDefinition("someDummyValue","ebay","1499817600000","1499817600000","someDummyValue")
    }
  }


  "CatalogService" should "throw Exception if materialization is set to true for level 0 view" in {
    val restClientMock = mock[RestService]
    val propertiesUtilMock = mock[PropertiesUtil]
    val catalogService: CatalogService = new CatalogServiceImpl{
      override def restClient = restClientMock
      override def propertiesUtil = propertiesUtilMock
    }
    (propertiesUtilMock.getProperty _).expects(Consts.CATALOG_SERVICE_BASE_URL_KEY).returning("http://host298.assist.pool.sv2.247-inc.net:8080/catalog")
    (propertiesUtilMock.getProperty _).expects(Consts.CATALOG_GET_VIEW_DEFN_API).returning("/processor/snapshot")
    (propertiesUtilMock.getProperty _).expects(Consts.DEBUG_VIEW_LOCATION).returning("false")
    val level0ViewUrl = new URL("http://host298.assist.pool.sv2.247-inc.net:8080/catalog/processor/snapshot?viewname=rawIdm&clientname=ebay&scheduleStartTime=1499817600000&scheduleEndTime=1499817600000&finalview=rawIdm")
    (restClientMock.sendGetRequest _).expects(level0ViewUrl).returning("{\n  \"viewName\": \"RawIDM\",\n  \"path\": \"/raw/prod/rtdp/idm/events/sainsburysargos/\",\n " +
      "\"preferReadMaterializedData\":false,\n \"format\": \"avro\",\n  \"referenceTime\": \"-60\",\n  \"timeDimensionColumn\": \"timeEpochMillisUTC\",\n  \"uniqueFields\": [\"header.timeEpochMillisUTC\", \"header.clientId\", \"header.channelSessionId\"],\n  \"importsList\": [],\n  \"sqls\": [],\n  \"materializationEnabled\": true,\n  \"dqEnabled\":true\n}   ")

    assertThrows[GenericCatalogException] {
      catalogService.getViewDefinition("rawIdm","ebay","1499817600000","1499817600000","rawIdm")
    }
  }

  //Set to dqenable is false and materialisationenabled id true ?
  "CatalogService" should "DqEnabled and MaterializationEnabled should be set to None for Internal views even if catalog returned valid Boolean" in {
    val restClientMock = mock[RestService]
    val propertiesUtilMock = mock[PropertiesUtil]
    val catalogService: CatalogService = new CatalogServiceImpl{
      override def restClient = restClientMock
      override def propertiesUtil = propertiesUtilMock
    }
    val expectedPt: PhysicalTable = PhysicalTable("test",
      Location("/reports/test1/%s/", "startTime"),
      Serde.JSON,
      Map("ebay" -> "eBay"),
      Map("abc"->"123"),
      Some(Consts.GENERIC_SOURCE_PLUGIN_MAIN_CLASS),
      Option(TableBuilder(
        Seq(ImportStatic(PhysicalTable("rawIdm", Location("/raw/prod/rtdp/idm/events/%s/", null), Serde.AVRO, Map("ebay" -> "eBay")),"201704080100", "201704080500")),
        SQLTableBuilderPlugInConf(SpartanPluginTypes.SQL_PROCESSOR.toString, Consts.GENERIC_SPARK_SQL_PLUGIN_MAIN_CLASS,Seq(SQL("test","select header.*, body.typeSpecificBody.body.Treatment.*, body.typeSpecificBody.body.Treatment.deflection.*, body.typeSpecificBody.body.Treatment.reportDimensions.* from rawIdm where body.typeSpecificBody.body.Treatment is not null")))
      )),null,Some(Consts.DEFAULT_VIEW_DQ_ENABLED),Some(Consts.DEFAULT_VIEW_MATERIALIZATION_ENABLED))

    (propertiesUtilMock.getProperty _).expects(Consts.CATALOG_SERVICE_BASE_URL_KEY).returning("http://host298.assist.pool.sv2.247-inc.net:8080/catalog").twice()
    (propertiesUtilMock.getProperty _).expects(Consts.CATALOG_GET_VIEW_DEFN_API).returning("/processor/snapshot").twice()
    (propertiesUtilMock.getProperty _).expects(Consts.DEBUG_VIEW_LOCATION).returning("false").twice()

    val testViewUrl = new URL("http://host298.assist.pool.sv2.247-inc.net:8080/catalog/processor/snapshot?viewname=test&clientname=ebay&scheduleStartTime=1491613200000&scheduleEndTime=1491616800000&finalview=test")
    val rawIdmUrl = new URL("http://host298.assist.pool.sv2.247-inc.net:8080/catalog/processor/snapshot?viewname=rawIdm&clientname=ebay&scheduleStartTime=1491613200000&scheduleEndTime=1491616800000&finalview=test")
    val testViewResponse = "{\n  \"viewName\" : \"test\",\n  \"path\" : \"/reports/test1/%s/\",\n  \"format\" : \"JSON\",\n  \"loadStrategy\" : \"RELATIVE\",\n  \"timeDimensionColumn\" : \"startTime\",\n  \"importsList\" : [{\"viewName\":\"rawIdm\",\"startTime\":\"1491613200000\",\"endTime\":\"1491616800000\",],\n  \"clientOverrideMap\":{\"ebay\":\"eBay\"},\n  \"sqls\" : [\n    {\"test\" : \"select header.*, body.typeSpecificBody.body.Treatment.*, body.typeSpecificBody.body.Treatment.deflection.*, body.typeSpecificBody.body.Treatment.reportDimensions.* from rawIdm where body.typeSpecificBody.body.Treatment is not null\"}\n  ]\n}"
    val rawIdmResponse = "{\n  \"viewName\" : \"rawIdm\",\n  \"path\" : \"/raw/prod/rtdp/idm/events/%s/\",\n  \"format\" : \"AVRO\",\n  \"loadStrategy\" : \"RELATIVE\",\n  \"clientOverrideMap\":{\"ebay\":\"eBay\"},\"materializationEnabled\": false,\n  \"dqEnabled\":false\n}"

    (restClientMock.sendGetRequest _).expects(testViewUrl).returning(testViewResponse)
    (restClientMock.sendGetRequest _).expects(rawIdmUrl).returning(rawIdmResponse)
    val actualPt: PhysicalTable = catalogService.getViewDefinition("test","ebay","1491613200000","1491616800000","test")
    assert(actualPt.dqEnable == None )
    assert(actualPt.materializationEnabled == None )
  }


  "CatalogService " should " get materiazed data for subsequent view and if MaterializationEnabled is not enabled it should throw and exception" in {
    val restClientMock = mock[RestService]
    val propertiesUtilMock = mock[PropertiesUtil]
    val catalogService: CatalogService = new CatalogServiceImpl {
      override def restClient = restClientMock

      override def propertiesUtil = propertiesUtilMock
    }
    val expectedPt: PhysicalTable = PhysicalTable("test",
      Location("/reports/test1/%s/", "startTime"),
      Serde.JSON,
      Map("ebay" -> "eBay"),
      Map("abc"->"123"),
      Some(Consts.GENERIC_SOURCE_PLUGIN_MAIN_CLASS),
      Option(TableBuilder(
        Seq(ImportStatic(PhysicalTable("View_InteractiveChatAnomalyDetection_310", Location("/Reports/prod/", null), Serde.JSON, Map("searsonline" -> "searsonline")), "201812031100", "201812031200")),
        SQLTableBuilderPlugInConf(SpartanPluginTypes.SQL_PROCESSOR.toString, Consts.GENERIC_SPARK_SQL_PLUGIN_MAIN_CLASS, Seq(SQL("View_InteractiveChatAnomalyDetection_310", "Select  unix_timestamp()*1000 as TimeField,CheckDay,PastData, aInteractiveChats ,bInteractiveChats from  View_InteractiveChatCount_310")))
      )), null, Some(Consts.READ_MATERIALIZATION_ENABLED),Some(Consts.DEFAULT_VIEW_DQ_ENABLED), Consts.DEFAULT_LOAD_STATEGY, Some(Consts.DEFAULT_VIEW_MATERIALIZATION_ENABLED))

    (propertiesUtilMock.getProperty _).expects(Consts.CATALOG_SERVICE_BASE_URL_KEY).returning("http://host298.assist.pool.sv2.247-inc.net:8080/catalog").twice()
    (propertiesUtilMock.getProperty _).expects(Consts.CATALOG_GET_VIEW_DEFN_API).returning("/processor/snapshot").twice()
    (propertiesUtilMock.getProperty _).expects(Consts.DEBUG_VIEW_LOCATION).returning("false").twice()

    val testViewUrl = new URL("http://host298.assist.pool.sv2.247-inc.net:8080/catalog/processor/snapshot?viewname=View_InteractiveChatAnomalyDetection_310&clientname=searsonline&scheduleStartTime=1543834800000&scheduleEndTime=1543838400000&finalview=View_InteractiveChatAnomalyDetection_310")
    val testViewResponse = "{\n  \"viewName\": \"View_InteractiveChatAnomalyDetection_310\",\n  \"path\": \"reports\test\",\n  \"format\": \"json\",\n  \"referenceTime\": \"-60\",\n  \"timeDimensionColumn\": \"TimeField\",\n  \"dqEnabled\": false,\n  \"materializationEnabled\": true,\n  \"processingPlugInType\": \"SQL\",\n  \"processorPlugInMainClass\": null,\n  \"readMatEnabled\":true,\n  \"uniqueFields\": [\n    \"CheckDay\"\n  ],\n  \"importsList\": [\n    {\n      \"viewName\": \"View_InteractiveChatCount_310\",\n      \"startTime\": \"1543836600000\",\n      \"endTime\": \"1543840200000\"\n    },\n    {\n      \"viewName\": \"View_InteractiveChatCount_310\",\n      \"startTime\": \"1543833000000\",\n      \"endTime\": \"1543836600000\"\n    }\n  ],\n  \"sqls\": [\n    {\n      \"View_InteractiveChatAnomalyDetection_310\": \"Select  unix_timestamp()*1000 as TimeField,CheckDay,PastData, aInteractiveChats ,bInteractiveChats from  View_InteractiveChatCount_310\"\n    }\n  ]\n}"
   (restClientMock.sendGetRequest _).expects(testViewUrl).returning(testViewResponse)
    val actualPt: PhysicalTable = catalogService.getViewDefinition("View_InteractiveChatAnomalyDetection_310", "searsonline", "1543834800000","1543838400000", "View_InteractiveChatAnomalyDetection_310")
    assert(expectedPt == actualPt)
  }


  it should "Extract list of columns from the given json string" in {
    val jsonStr = "{\"columns\": [{\"columnName\": \"uniqueRowId\",\"isSensitive\": 0},{\"columnName\": \"journeyStartTime\",\"isSensitive\": 1}]}"
    val jsonObj = JsonParser.parse(jsonStr)
    val catalogService:CatalogServiceImpl = new CatalogServiceImpl
    val cols = catalogService.getColumns(jsonObj)
    assert(cols.length == 2)
    assert(cols(0).isSensitive == false)
    assert(cols(1).isSensitive == true)
  }

  //Custom params
  "CatalogService" should "send customParams as part of processor snapshot and validate customParams is coming in physicalTable " in {
    val restClientMock = mock[RestService]
    val propertiesUtilMock = mock[PropertiesUtil]
    val catalogService: CatalogService = new CatalogServiceImpl{
      override def restClient = restClientMock
      override def propertiesUtil = propertiesUtilMock
    }
    val expectedPt: PhysicalTable = PhysicalTable("test",
      Location("/reports/test1/%s/", "startTime"),
      Serde.JSON,
      Map("ebay" -> "eBay"),
      Map("abc"->"123"),
      Some(Consts.GENERIC_SOURCE_PLUGIN_MAIN_CLASS),
      Option(TableBuilder(
        Seq(ImportStatic(PhysicalTable("rawIdm", Location("/raw/prod/rtdp/idm/events/%s/", null), Serde.AVRO, Map("ebay" -> "eBay")),"201704080100", "201704080500")),
        SQLTableBuilderPlugInConf(SpartanPluginTypes.SQL_PROCESSOR.toString, Consts.GENERIC_SPARK_SQL_PLUGIN_MAIN_CLASS,Seq(SQL("test","select header.*, body.typeSpecificBody.body.Treatment.*, body.typeSpecificBody.body.Treatment.deflection.*, body.typeSpecificBody.body.Treatment.reportDimensions.* from rawIdm where body.typeSpecificBody.body.Treatment is not null")))
      )),null,Some(Consts.DEFAULT_VIEW_DQ_ENABLED),Some(Consts.DEFAULT_VIEW_MATERIALIZATION_ENABLED),Consts.DEFAULT_LOAD_STATEGY, Option(false),null, Some(Map("L1" -> "AvroTable")), Some("<yyyy-mm-dd>"))

    (propertiesUtilMock.getProperty _).expects(Consts.CATALOG_SERVICE_BASE_URL_KEY).returning("http://host298.assist.pool.sv2.247-inc.net:8080/catalog").twice()
    (propertiesUtilMock.getProperty _).expects(Consts.CATALOG_GET_VIEW_DEFN_API).returning("/processor/snapshot").twice()
    (propertiesUtilMock.getProperty _).expects(Consts.DEBUG_VIEW_LOCATION).returning("false").twice()

    val testViewUrl = new URL("http://host298.assist.pool.sv2.247-inc.net:8080/catalog/processor/snapshot?viewname=test&clientname=ebay&scheduleStartTime=1491613200000&scheduleEndTime=1491627600000&finalview=test")
    val rawIdmUrl = new URL("http://host298.assist.pool.sv2.247-inc.net:8080/catalog/processor/snapshot?viewname=rawIdm&clientname=ebay&scheduleStartTime=1491613200000&scheduleEndTime=1491627600000&finalview=test")
    val testViewResponse = "{\n  \"viewName\" : \"test\",\n  \"path\" : \"/reports/test1/%s/\",\n  \"format\" : \"JSON\",\n  \"loadStrategy\" : \"RELATIVE\",\n  \"timeDimensionColumn\" : \"startTime\",\n  \"importsList\" : [{\"viewName\":\"rawIdm\",\"startTime\":\"1491613200000\",\"endTime\":\"1491627600000\",\"timeFormat\":\"yyyy-MM-dd HH:mm\"}],\n  \"clientOverrideMap\":{\"ebay\":\"eBay\"},\n  \"sqls\" : [\n    {\"test\" : \"select header.*, body.typeSpecificBody.body.Treatment.*, body.typeSpecificBody.body.Treatment.deflection.*, body.typeSpecificBody.body.Treatment.reportDimensions.* from rawIdm where body.typeSpecificBody.body.Treatment is not null\"}\n  ], \"customParams\":{\"L1\":\"AvroTable\"}\n}"



    val rawIdmResponse = "{\n  \"viewName\" : \"rawIdm\",\n  \"path\" : \"/raw/prod/rtdp/idm/events/%s/\",\n  \"format\" : \"AVRO\",\n  \"loadStrategy\" : \"RELATIVE\",\n  \"clientOverrideMap\":{\"ebay\":\"eBay\"},\"materializationEnabled\": false,\n  \"dqEnabled\":false\n , \"customParams\":{}}"


    (restClientMock.sendGetRequest _).expects(testViewUrl).returning(testViewResponse)
    (restClientMock.sendGetRequest _).expects(rawIdmUrl).returning(rawIdmResponse)
    val actualPt: PhysicalTable = catalogService.getViewDefinition("test","ebay","1491613200000","1491627600000", "test")
    assert(actualPt.customParams.get.contains("L1"))

  }

}
