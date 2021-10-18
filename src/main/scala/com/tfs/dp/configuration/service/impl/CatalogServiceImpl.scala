package com.tfs.dp.configuration.service.impl

import java.net.URL

import com.github.nscala_time.time.Imports._
import com.tfs.dp.configuration.service.CatalogService
import com.tfs.dp.exceptions.{GenericCatalogException, JsonParseException, NoDQDefinedException}
import com.tfs.dp.rest.{RestClient, RestService}
import com.tfs.dp.spartan.conf.ViewInputLoadStrategy.ViewInputLoadStrategy
import com.tfs.dp.spartan.conf.{Consts, ViewInputLoadStrategy}
import com.tfs.dp.spartan.dg.DGCatalogModel._
import com.tfs.dp.spartan.dg.{DGCatalogModel, DGTableDQDef, DQDataSet, DQRule}
import com.tfs.dp.spartan.utils.PropertiesUtil
import net.liftweb.json
import net.liftweb.json.JsonAST.JValue
import net.liftweb.json.{DefaultFormats, JsonParser}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql._
import org.joda.time.DateTimeZone

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by devinder.k on 12/5/2017.
  */
class CatalogServiceImpl extends CatalogService with Logging {
  def restClient: RestService = RestClient

  def propertiesUtil: PropertiesUtil = PropertiesUtil

  val tableList: mutable.Set[String] = mutable.Set[String]()
  var preferMaterializedData: Option[Boolean] = Some(false)

  def getViewDefinitionFromCatalog(viewName: String, clientName: String, scheduleStartTime: String, scheduleEndTime: String, finalView: String, isDevMode: Boolean): String = {
    val catalogServiceHost = propertiesUtil.getProperty(Consts.CATALOG_SERVICE_BASE_URL_KEY)
    val getViewDefnAPI = propertiesUtil.getProperty(Consts.CATALOG_GET_VIEW_DEFN_API)
    val devViewLocation = propertiesUtil.getProperty(Consts.DEBUG_VIEW_LOCATION)
    val requestParameters = getViewDefnAPI + "?viewname=" + viewName + "&clientname=" + clientName +
      "&scheduleStartTime=" + scheduleStartTime + "&scheduleEndTime=" + scheduleEndTime + "&finalview=" + finalView
    val url = new URL(catalogServiceHost + requestParameters)
    if (isDevMode) {
      val df = SparkSession.builder().appName("SpartanApp").getOrCreate().read.text(devViewLocation + s"$viewName.json")
      var str: StringBuilder = new StringBuilder("");
      df.collect.foreach(x =>
        str.append(x.getString(0))
      )
      str.toString()
    } else {
      logger.info("Sending GET Request for URL: " + url.toString)
      restClient.sendGetRequest(url)
    }
  }

  def getDataLoadStrategy(jsonObj: json.JValue): ViewInputLoadStrategy = {
    implicit val formats = DefaultFormats
    try {
      ViewInputLoadStrategy.withName((jsonObj \\ "loadStrategy").extract[String])
    } catch {
      case e: Exception =>
        logger.error(s"Error parsing value for loadStrategy,setting it to default value : RELATIVE")
        Consts.DEFAULT_LOAD_STATEGY
    }
  }

  override def getViewDefinition(viewName: String, clientName: String, scheduleStartTime: String, scheduleEndTime:String, finalView: String, isDevMode: Boolean = false): PhysicalTable = {
    try {
      implicit val formats = DefaultFormats
      val jsonResponse = getViewDefinitionFromCatalog(viewName, clientName, scheduleStartTime, scheduleEndTime, finalView, isDevMode)
      logger.info("Received JSON Response is :" + jsonResponse)

      val isFinalView: Boolean = viewName.equals(finalView)
      val isSubsequentView: Boolean = tableList.contains(viewName)
      val jsonObj = JsonParser.parse(jsonResponse)

      val preferReadMaterializedData = (jsonObj \\ "preferReadMaterializedData").extractOpt[Boolean]
      if (isFinalView) {
        preferMaterializedData = preferReadMaterializedData
      }

      var inputDataLoadStrategy: ViewInputLoadStrategy = getDataLoadStrategy(jsonObj)

      val imports_jValue = (jsonObj \\ "importsList").children
      for (imprt <- imports_jValue) {
        val imprtViewName = (imprt \\ "viewName").extract[String]
        logger.info(s"Enabling materialized data reading for subsequent view ${imprtViewName}")
        tableList += (imprtViewName)
      }

      var view_name = viewName

      val location_jValue = (jsonObj \\ "path")
      val location = location_jValue.extract[String]

      val format_jValue = (jsonObj \\ "format")
      val format = format_jValue.extract[String]

      val dqEnabled_jValue = (jsonObj \\ "dqEnabled")
      val dqEnabled = dqEnabled_jValue.extractOpt[Boolean]

      val materializationEnabled_jValue = (jsonObj \\ "materializationEnabled")
      val materializationEnabled = materializationEnabled_jValue.extractOpt[Boolean]

      var epochColumn: String = null
      try {
        val epochColumn_jValue = (jsonObj \\ "timeDimensionColumn")
        epochColumn = epochColumn_jValue.extract[String]
      } catch {
        case e: Exception =>
          if (isFinalView) {
            logger.error("Final view  must have timedimensioncolumn")
            throw JsonParseException(s"Error while parsing JSON response for view $viewName from catalogService ")
          }
      }

      val uniqueFields_jvalue = (jsonObj \\ "uniqueFields").children
      val uniqueFields = ArrayBuffer[String]()
      for (uField <- uniqueFields_jvalue) {
        uniqueFields += uField.extract[String]
      }

      val imports: ArrayBuffer[Import] = getDependentViews(jsonObj,
        scheduleStartTime,
        scheduleEndTime,
        viewName,
        clientName,
        finalView,
        isDevMode,
        isSubsequentView,
        materializationEnabled)

      val sqls = ArrayBuffer[SQL]()
      val sqls_jValue = (jsonObj \\ "sqls").children
      for (sqlObj <- sqls_jValue) {
        val sql = sqlObj.values.asInstanceOf[Map[String, String]]
        for (sqlPair <- sql)
          sqls += SQL(sqlPair._1, sqlPair._2)
      }

      val clientOverRide_jValue = (jsonObj \\ "clientOverrideMap")
      val clientOverrideMap: Map[String, String] = clientOverRide_jValue.values.asInstanceOf[Map[String, String]]

      if (0 == imports.size && materializationEnabled.equals(Some(true)) && preferMaterializedData.equals(Some(false))) {
        throw GenericCatalogException(s"Materialization Can not be enabled true for level 0 view: $viewName")
      }

      var sList: Map[String, String] = Map()
      val sourceConfigList_jvalue = (jsonObj \\ "sourceConfigList")
      sList = sourceConfigList_jvalue.values.asInstanceOf[Map[String, String]]

      //Fetch Params  from catalog
      var customParams: Option[Map[String, String]] = Some(Map())
      customParams = (jsonObj \\ "customParams").extractOpt[Map[String, String]]
      val sourcePluginClass = (jsonObj \\ "sourcePluginClass").extractOpt[String]
      val dynamicBucketPath = (jsonObj \\ "dynamicBucketPath").extractOpt[String]
      var tableBuilderPlugInType: String = null

      try {
        tableBuilderPlugInType = (jsonObj \\ "processingPlugInType").extract[String]
      } catch {
        case ex: Exception =>
          logger.warn("The processingPlugInType may not have been configured yet. For backward compatibility, " +
            "setting it to SQL")
          tableBuilderPlugInType = DGCatalogModel.SpartanPluginTypes.SQL_PROCESSOR.toString
      }

      logger.info(s"Table builder plug in type configured in the catalog - ${tableBuilderPlugInType}")
      //logger.info(s"Table builder plug in main class configued in the catalog - ${(jsonObj \\ "processorPlugInMainClass").extract[String]}")

      var tableBuilderPlugInConf: DGCatalogModel.GenericTableBuilderPlugInConf = null
      if (SpartanPluginTypes.withName(tableBuilderPlugInType) == DGCatalogModel.SpartanPluginTypes.SCALA_PROCESSOR) {
        tableBuilderPlugInConf = DGCatalogModel.ScalaTableBuilderPlugInConf(
          DGCatalogModel.SpartanPluginTypes.SCALA_PROCESSOR.toString, (jsonObj \\ "processorPlugInMainClass").extract[String])
      } else if (SpartanPluginTypes.withName(tableBuilderPlugInType) == DGCatalogModel.SpartanPluginTypes.SQL_PROCESSOR) {
        tableBuilderPlugInConf = DGCatalogModel.SQLTableBuilderPlugInConf(
          DGCatalogModel.SpartanPluginTypes.SQL_PROCESSOR.toString, Consts.GENERIC_SPARK_SQL_PLUGIN_MAIN_CLASS, sqls)
      } else {
        logger.warn("View processing plugin type is not configured / invalid. Falling back to SQL to support backward " +
          "compatibility")
        tableBuilderPlugInConf = DGCatalogModel.SQLTableBuilderPlugInConf(
          DGCatalogModel.SpartanPluginTypes.SQL_PROCESSOR.toString, Consts.GENERIC_SPARK_SQL_PLUGIN_MAIN_CLASS, sqls)
      }

      val tableBuilder = if (0 == imports.size) None else Option(TableBuilder(imports, tableBuilderPlugInConf))
      val deduplication = if (0 == uniqueFields.size) null else RawEventDeduplicator(uniqueFields)


      var pt: PhysicalTable = null
      val materiliznEnabled = if (!materializationEnabled.isEmpty && !materializationEnabled.equals(Some(null))) materializationEnabled
      else Some(Consts.DEFAULT_VIEW_MATERIALIZATION_ENABLED)

      if (viewName.equals(finalView)) {
        val dqEnabledFlag = if (!dqEnabled.isEmpty && !dqEnabled.equals(Some(null))) dqEnabled else Some(Consts.DEFAULT_VIEW_DQ_ENABLED)

        pt = PhysicalTable(
          view_name, Location(location, epochColumn), Serde.withName(format.toUpperCase),
          clientOverrideMap, sList, sourcePluginClass, tableBuilder, deduplication,
          preferMaterializedData, materiliznEnabled, inputDataLoadStrategy, dqEnabledFlag, Option(getColumns(jsonObj)), customParams, dynamicBucketPath)

      }
      else {
        pt = PhysicalTable(
          view_name, Location(location, epochColumn), Serde.withName(format.toUpperCase),
          clientOverrideMap, sList, sourcePluginClass, tableBuilder, deduplication,
          preferMaterializedData, materiliznEnabled, inputDataLoadStrategy, None, Option(getColumns(jsonObj)), customParams, dynamicBucketPath)

      }
      logger.info(s"SuccessFully serialized view definition for view $viewName")
      pt
    } catch {
      case ex: GenericCatalogException =>
        logger.error(s"Catalog Data Exception for view $viewName", ex)
        throw ex
      case ex: Exception =>
        logger.error(s"Error while parsing JSON response for view $viewName", ex)
        throw JsonParseException(s"Error while parsing JSON response for view $viewName from catalogService ")
    }
  }

  private def getDependentViews(jsonObj: JValue,
                                scheduleStartTime: String,
                                scheduleEndTime:String,
                                viewName: String,
                                clientName: String,
                                finalView: String,
                                isDevMode: Boolean
                                , isSubsequentView: Boolean,
                                materializationEnabled: Option[Boolean]): ArrayBuffer[Import] = {

    var importList = ArrayBuffer[Import]()

    if (viewName.equals(finalView)) {
      // Final view, so always get the childs
      importList = getImportList(jsonObj, scheduleStartTime, scheduleEndTime, clientName, finalView, isDevMode)
    }
    else if (isSubsequentView) {
      // child view
      if (materializationEnabled.equals(Some(false))) {
        // child view had no materializatin enabled, so get its childs
        importList = getImportList(jsonObj, scheduleStartTime, scheduleEndTime, clientName, finalView, isDevMode)
      }
      else if (preferMaterializedData.equals(Some(false))) {
        // child view has materialization enabled but parent is not interested in materialized view, get its child
        importList = getImportList(jsonObj, scheduleStartTime, scheduleEndTime, clientName, finalView, isDevMode)
      }
    }

    importList
  }

  private def dateToPartitionIdFormat(dateTime: DateTime): String = {
    "" + dateTime.getYear +
      "" + (if (dateTime.getMonthOfYear < 10) "0" + dateTime.getMonthOfYear else dateTime.getMonthOfYear) +
      "" + (if (dateTime.getDayOfMonth < 10) "0" + dateTime.getDayOfMonth else dateTime.getDayOfMonth) +
      "" + (if (dateTime.getHourOfDay < 10) "0" + dateTime.getHourOfDay else dateTime.getHourOfDay) +
      "" + (if (dateTime.getMinuteOfHour < 10) "0" + dateTime.getMinuteOfHour else dateTime.getMinuteOfHour)
  }

  private def getImportList(jsonObj: JValue, scheduleStartTime: String, scheduleEndTime: String, clientName: String, finalView: String, isDevMode: Boolean): ArrayBuffer[Import] = {
    implicit val formats = DefaultFormats
    val imports_jValue = (jsonObj \\ "importsList").children
    val imports = ArrayBuffer[Import]()
    for (imprt <- imports_jValue) {
      val imprtViewName = (imprt \\ "viewName").extract[String]
      val imprtStartDateString = (imprt \\ "startTime").extract[String]
      val imprtEndDateString = (imprt \\ "endTime").extract[String]

      val startDateEpoch = if (null != imprtStartDateString) imprtStartDateString.toLong else scheduleStartTime.toLong
      val endDateEpoch = if (null != imprtEndDateString) imprtEndDateString.toLong else scheduleEndTime.toLong
      imports += ImportStatic(
        getViewDefinition(
          imprtViewName,
          clientName,
          startDateEpoch.toString,
          endDateEpoch.toString,
          finalView,
          isDevMode
        ),
        dateToPartitionIdFormat(new DateTime(startDateEpoch, DateTimeZone.UTC)),
        dateToPartitionIdFormat(new DateTime(endDateEpoch, DateTimeZone.UTC))
      )
    }
    imports
  }

  override def getDQDef(viewName: String): DGTableDQDef = {
    implicit val formats = DefaultFormats
    try {
      val response = catalogRequest(s"${viewName}/${propertiesUtil.getProperty(Consts.CATALOG_GET_DQ_DEFN_API)}")

      if (response.isEmpty) throw new NoDQDefinedException(s"No DQ defined for the view - ${viewName}")

      logger.info(s"DQ Def response received from catalg: ${response}")
      val dqDefJsonObj = JsonParser.parse(response)
      val dqDatasetsjValue = (dqDefJsonObj \\ "dqDatasetList").children
      var dqDataSets = ListBuffer[DQDataSet]()
      for (dqDatasetjValue <- dqDatasetsjValue) {
        val ruleListjValue = (dqDatasetjValue \\ "ruleList").children
        val rules = ListBuffer[DQRule]()
        for (rulejValue <- ruleListjValue) {
          rules += DQRule((rulejValue \\ "ruleId").extract[String], (rulejValue \\ "ruleName").extract[String],
            (rulejValue \\ "ruleSql").extract[String], (rulejValue \\ "description").extract[String])
        }
        dqDataSets += DQDataSet((dqDatasetjValue \\ "datasetId").extract[String],
          (dqDatasetjValue \\ "datasetName").extract[String],
          (dqDatasetjValue \\ "datasetSql").extract[String],
          (dqDatasetjValue \\ "recordIdentifier").extract[String],
          (dqDatasetjValue \\ "datasetDescription").extract[String],
          (dqDatasetjValue \\ "level").extract[String], rules)
      }
      new DGTableDQDef(dqDataSets)
    } catch {
      case ex: NoDQDefinedException =>
        logger.error(s"No DQ defined for the view - ${viewName}")
        throw new NoDQDefinedException(s"No DQ defined for the view - ${viewName}")
      case ex: Exception =>
        logger.error(s"An exception occurred while fetching the DQ def for the view: $viewName", ex)
        throw GenericCatalogException(s"An exception occurred while fetching the DQ def for the view $viewName from catalogService")
    }
  }

  private def catalogRequest(uri: String): String = {
    //val requestURL = s"${propertiesUtil.getProperty(Consts.CATALOG_SERVICE_BASE_URL_KEY)}/${uri}"
    val requestURL = new URL(propertiesUtil.getProperty(Consts.CATALOG_SERVICE_BASE_URL_KEY) + "/" + uri)
    try {
      logger.info(s"Sending a catalog request: ${requestURL}")
      restClient.sendGetRequest(requestURL)
    } catch {
      case ex: Exception =>
        logger.error(s"Catalog request exception: $uri", ex)
        throw GenericCatalogException(s"An exception occurred while querying catalog at the URL: ${requestURL}")
    }
  }

  def getColumns(viewJsonObj: JValue) = {
    var cols = Seq[DGCatalogModel.TableColumn]()
    implicit val formats = DefaultFormats
    val colsJsonArrayObj = (viewJsonObj \\ "columns").children
    for (colJsonObj <- colsJsonArrayObj) {
      val colName = (colJsonObj \\ "columnName").extract[String]
      val isSensitive = (colJsonObj \\ "isSensitive").extract[Int]
      cols = cols :+ TableColumn(colName, if (isSensitive == 1) true else false)
    }
    cols
  }
}