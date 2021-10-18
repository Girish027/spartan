package com.tfs.dp.spartan.dg


import java.text.SimpleDateFormat

import com.github.nscala_time.time.Imports._
import com.tfs.dp.crypto.EncryptionUtil
import com.tfs.dp.encoding.EncoderUtil
import com.tfs.dp.exceptions.{DataReadException, DedupException, SpartanPluginException}
import com.tfs.dp.spartan.Utils
import com.tfs.dp.spartan.conf.Consts._
import com.tfs.dp.spartan.conf.ViewInputLoadStrategy.ViewInputLoadStrategy
import com.tfs.dp.spartan.conf.{Consts, ViewInputLoadStrategy}
import com.tfs.dp.spartan.manager.CustomOutputPartitioner
import com.tfs.dp.spartan.plugins.SpartanProcessorPlugin
import com.tfs.dp.spartan.plugins.sourceConfig.SourceAdapterPlugin
import com.tfs.dp.spartan.utils.PropertiesUtil
import com.tfs.dp.spartan.utils.kafka.KafkaWriter
import net.liftweb.json.Serialization.write
import org.apache.logging.log4j.ThreadContext
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, ColumnName, DataFrame, SparkSession}
import org.joda.time.DateTime

import scala.util.Try

/**
  * Created by guruprasad.gv on 8/27/17.
  */
object DGCatalogModel extends Logging{

  def propertiesUtil: PropertiesUtil = PropertiesUtil
  val schemaLocation = propertiesUtil.getProperty(SCHEMA_LOCATION_KEY)
  object Serde extends Enumeration {
    val AVRO = Value("AVRO")
    val JSON = Value("JSON")
    val PARQUET = Value("PARQUET")
    val ORC = Value("ORC")
    val ASSIST_INTERACTIONS = Value("ASSIST_INTERACTIONS")
    val ASSIST_AGENTSTATS = Value("ASSIST_AGENTSTATS")
    val ASSIST_INTERACTIONS_V2 = Value("ASSIST_INTERACTIONS_V2")
    val ASSIST_TRANSCRIPTS = Value("ASSIST_TRANSCRIPTS")
    val SEQ_JSON = Value("SEQ_JSON")
    val CSV = Value("CSV")
    val ACTIVE_SHARE = Value("ACTIVE_SHARE")
    val SESSIONIZED_BINLOG = Value("SESSIONIZED_BINLOG")
  }
  var inputPathsList = scala.collection.mutable.Set[String]()

  case class SQL(outputRef: String, sql: String);

  abstract class Import

  /*This will be used in case Import Definition is retrived from Catalog.
  * In that case startPartition and endPartitions will be static timestamps*/
  case class ImportStatic(table: PhysicalTable, startPartition: String, endPartition: String) extends Import {

    def this(tableName: String, startPartition: String, endPartition: String) =
      this((DGCatalogRegistry.registry.get(tableName).get), startPartition, endPartition);

    override def toString: String = {
      s"${table.tableName}:(${startPartition},${endPartition})"
    }
  }

  /* This will be used in case Import Definition is retrived from Zeppelin notebook
  * In this Case start partition and end partition will be relative i.e. eg "-1.days" from the parent view
  * startPartition offset */
  case class ImportRelative(table: PhysicalTable, startPartition: Period, endPartition: Period) extends Import {

    def this(tableName: String, startPartition: Period, endPartition: Period) =
      this((DGCatalogRegistry.registry.get(tableName).get), startPartition, endPartition);

    def applyStartOffset(startDate: DateTime): DateTime = {
      startDate.plus(startPartition)
    }
    def applyEndOffset(endDate: DateTime): DateTime = {
      endDate.plus(endPartition)
    }
    override def toString: String = {
      s"${table.tableName}:(${startPartition},${endPartition})"
    }
  }

  case class TableBuilder(imports: Seq[Import], tableBuilderPlugInConf: GenericTableBuilderPlugInConf)

  abstract class GenericTableBuilderPlugInConf {
    val plugInConfType: String
    val plugInMainClass: String
  }

  object SpartanPluginTypes extends Enumeration {
    type SpartanPluginTypes = Value

    val SCALA_PROCESSOR = Value("SCALA")
    val SQL_PROCESSOR = Value("SQL")
  }

  case class ScalaTableBuilderPlugInConf(plugInConfType: String, plugInMainClass: String) extends GenericTableBuilderPlugInConf

  case class SQLTableBuilderPlugInConf(plugInConfType: String, plugInMainClass: String, sqls: Seq[SQL]) extends GenericTableBuilderPlugInConf

  case class Location(uriFormat: String, epochTimeColumn: String = null)

  case class RawEventDeduplicator(seqOfFields: Seq[String])

  case class TableColumn(name:String, isSensitive:Boolean)
  case class PhysicalTable(tableName: String, location: Location, serde: Serde.Value = Serde.AVRO,
                           clientOverrideMap: Map[String, String],
                           sourceConf: Map[String, String] = null,
                           sourcePluginClass: Option[String] = None,
                           tableBuilder: Option[TableBuilder] = None,
                           deduplication: RawEventDeduplicator = null,
                           readMaterializedDataEnabled:Option[Boolean] = None,
                           materializationEnabled:Option[Boolean] = None,
                           inputDataLoadStrategy: ViewInputLoadStrategy = Consts.DEFAULT_LOAD_STATEGY,
                           dqEnable: Option[Boolean]= None,
                           columns:Option[Seq[TableColumn]]=Option(Seq()),
                           customParams: Option[Map[String, String]] = None,
                           dynamicBucketPath: Option[String] = None
                          ) extends Logging {

    var sourceAdapterMainClass: String = null
    serde match {
      case Serde.AVRO =>
        sourceAdapterMainClass = Consts.AVRO_SOURCE_ADAPTER_MAIN_CLASS
      case Serde.ASSIST_INTERACTIONS =>
        sourceAdapterMainClass = Consts.AssistInteraction_SOURCE_ADAPTER_MAIN_CLASS
      case Serde.ASSIST_AGENTSTATS =>
        sourceAdapterMainClass = Consts.AssistAgentStats_SOURCE_ADAPTER_MAIN_CLASS
      case Serde.ASSIST_INTERACTIONS_V2 =>
        sourceAdapterMainClass = Consts.AssistInteractionV2_SOURCE_ADAPTER_MAIN_CLASS
      case Serde.ASSIST_TRANSCRIPTS =>
        sourceAdapterMainClass = Consts.AssistTranscript_SOURCE_ADAPTER_MAIN_CLASS
      case Serde.SEQ_JSON =>
        sourceAdapterMainClass = Consts.JSONSeq_SOURCE_ADAPTER_MAIN_CLASS
      case Serde.JSON =>
        sourceAdapterMainClass = Consts.JSON_SOURCE_ADAPTER_MAIN_CLASS
      case Serde.CSV =>
        sourceAdapterMainClass = Consts.CSV_SOURCE_ADAPTER_MAIN_CLASS
      case Serde.ACTIVE_SHARE =>
        sourceAdapterMainClass = Consts.ActiveShare_SOURCE_ADAPTER_MAIN_CLASS
      case Serde.SESSIONIZED_BINLOG =>
        sourceAdapterMainClass = Consts.Sessionized_Binlog_SOURCE_ADAPTER_MAIN_CLASS
      case _ =>
        if (sourcePluginClass.nonEmpty)
          sourceAdapterMainClass = sourcePluginClass.toString
        else
          throw new IllegalArgumentException("Not supported serde " + serde)
    }


    def getPhysicalLocation(rootString: String, client: String): String = {
      val clientId = clientOverrideMap.get(client).getOrElse(client)
      var basePath = rootString + "/" + location.uriFormat.format(clientId)
     /* /*params.isEmpty*/
      if(!customParams.isEmpty)
        basePath = getParamsPhysicalLocation(basePath)*/

      basePath
    }

    def getPhysicalLocation(rootString: String, viewName: String, client: String): String = {
      val clientId = clientOverrideMap.get(client).getOrElse(client)
      rootString + "/" + location.uriFormat + "/" + "view=" + viewName + "/" + "client=" + clientId
    }

    def getPhysicalLocationUntillClient(rootString: String, viewName: String): String = {
      rootString + "/" + location.uriFormat + "/" + "view=" + viewName
    }

    /*
   Sample code to execute flattened Schema, may be useful when traversing through Parquet data
       val flattenedSchema = flattenSchema(tableDf.schema)
       val renamedCols = flattenedSchema.map(name => col(name.toString()).as(name.toString().replace(".","_")))
       var newTableDf = tableDf.select(renamedCols:_*)


    */

    def flattenSchema(schema: StructType, prefix: String = null) : Array[Column] = {
      schema.fields.flatMap(f => {
        val colName = if (prefix == null) f.name else (prefix + "." + f.name)

        f.dataType match {
          case st: StructType => flattenSchema(st, colName)
          case _ => Array(col(colName))
        }
      })
    }

    /**
      * For now, only leaf nodes are raw physical tables, everything else is logical.
      *
      * @param spark
      * @param client
      * @param rootPath
      * @param startPartitionId
      * @param endPartitionId
      */
    def createView(spark: SparkSession,
                   client: String, rootPath: String, startPartitionId: String,
                   endPartitionId: String, loadStrategy: ViewInputLoadStrategy, dynamicBucketPath :Option[String]): Unit = {
      val startDate = DateTime.parse(startPartitionId, partitionIdFormater);
      val endDate = DateTime.parse(endPartitionId, partitionIdFormater);
      val physicalLocation = getPhysicalLocation(rootPath, client)
      logger.info(s"Preparing Spark View for table: ${tableName}, StartDate: ${startDate}, EndDate: ${endDate}")

      //Read from materialized data only when
      if(readMaterializedDataEnabled.isDefined
        &&  readMaterializedDataEnabled.equals(Some(true))
        &&  materializationEnabled.isDefined
        &&  materializationEnabled.equals(Some(true))
        &&  tableBuilder.isEmpty){
        logger.info(s"Reading from materialized data for table/view: ${tableName}, StartDate: ${startDate}, EndDate: ${endDate}")
        val materializedDir = (getPhysicalLocationUntillClient(rootPath, tableName)).toString().concat("/client=").concat(client)
        val resultantString :String = Utils.getOutputFinalPath(materializedDir,startPartitionId,endPartitionId,Consts.defaultRawDataGranularity);
        var tableDf = loadMaterializedData(spark, resultantString, startPartitionId, endPartitionId)
        EncryptionUtil.decryptCols(spark, tableDf, Try(this.columns.get).getOrElse(Seq())).createOrReplaceTempView(tableName);
      }
      else if (tableBuilder.isEmpty) {
             var tableDf = loadRawData(spark, physicalLocation, startPartitionId, endPartitionId, sourceAdapterMainClass, loadStrategy, dynamicBucketPath)

        try {
          if (deduplication != null && !deduplication.seqOfFields.isEmpty) {
            logger.info("Inside df deduplication")
            // remove duplicates
            var i: Integer = 0
            var deDupFields: Seq[String] = Seq.empty[String]
            logger.info("Deduplication columns: " + deduplication.seqOfFields.toString())
            for (i <- 0 until deduplication.seqOfFields.length) {
              tableDf = tableDf.withColumn("dedup" + i, new ColumnName(deduplication.seqOfFields(i)))
              deDupFields = deDupFields :+ "dedup" + i
            }
            tableDf = tableDf.dropDuplicates(deDupFields)
          }
        } catch {
          case ex: Exception =>
            logger.error(s"Error during dedup phase for table: $tableName", ex)
            throw DedupException(s"Error during dedup phase for table: $tableName")
        }

        // repartition
        logger.info("Number of Partitions:" + tableDf.rdd.getNumPartitions)

        // creating view out off final data
        tableDf.createOrReplaceTempView(tableName);
        logger.info(s"Successfully Defined View Definition layer 0 for temp view: $tableName")
      }
      else {

        tableBuilder.get.imports.foreach(imprt => {
          /**
            * Currently only used for logical view, so no real value in using the partitions
            */
          createDependentViews(imprt, spark, rootPath, client, startPartitionId, endPartitionId)
        })

        invokeTableBuilderPlugin(spark, this)
      }
    }


    private def invokeTableBuilderPlugin(sparkSession: SparkSession, tableDef: PhysicalTable): Unit = {
      try {
        val plugInMainClass = tableDef.tableBuilder.get.tableBuilderPlugInConf.plugInMainClass
        logger.info(s"Invoking the spartan plugin with the main class - ${plugInMainClass}")
        val pluginInstance = Class.forName(plugInMainClass).newInstance().asInstanceOf[SpartanProcessorPlugin]
        pluginInstance.process(sparkSession, tableDef)
      } catch {
        case ex: Exception =>
          logger.error("Error invoking a spartan plugin.", ex)
          throw SpartanPluginException("Error invoking a spartan plugin.")
      }
    }

    private def createDependentViews(imprtView: Import, spark: SparkSession, rootPath: String, client: String,
                                     startPartitionId: String,
                                     endPartitionId: String) = {
      if (imprtView.isInstanceOf[ImportRelative]) {

        val startDate = DateTime.parse(startPartitionId, partitionIdFormater);
        val endDate = DateTime.parse(endPartitionId, partitionIdFormater);

        val imprtRelative = imprtView.asInstanceOf[ImportRelative]
        val startDateForTable = imprtRelative.applyStartOffset(startDate)
        val endDateForTable = imprtRelative.applyEndOffset(endDate)
        imprtRelative.table.createView(
          spark,
          client,
          rootPath,
          startDateForTable.toString(partitionIdFormater),
          endDateForTable.toString(partitionIdFormater),imprtRelative.table.inputDataLoadStrategy, imprtRelative.table.dynamicBucketPath);
      } else {
        val imprtStatic = imprtView.asInstanceOf[ImportStatic]
        val startTimePartition: String =
          if (null != imprtStatic.startPartition && "" != imprtStatic.startPartition) {
            imprtStatic.startPartition
          } else {
            startPartitionId
          }
        val endTimePartition: String =
          if (null != imprtStatic.endPartition && "" != imprtStatic.endPartition) {
            imprtStatic.endPartition
          } else {
            endPartitionId
          }
        imprtStatic.table.createView(spark, client, rootPath, startTimePartition, endTimePartition,imprtStatic.table.inputDataLoadStrategy, imprtStatic.table.dynamicBucketPath);
      }
    }

    private def loadMaterializedData(spark: SparkSession, physicalLocation: String, startPartitionId: String, endPartitionId: String): DataFrame = {
      logger.info(s"serde type is ${serde} while reading materialized data ")
      val startDate = DateTime.parse(startPartitionId, partitionIdFormater);
      val endDate = DateTime.parse(endPartitionId, partitionIdFormater);
      val tableDfval = try {
        serde match {
          case Serde.AVRO | Serde.ASSIST_INTERACTIONS | Serde.ASSIST_AGENTSTATS | Serde.ASSIST_INTERACTIONS_V2 | Serde.ASSIST_TRANSCRIPTS | Serde.SEQ_JSON | Serde.JSON =>
            readMaterializedRawData(spark, physicalLocation, startPartitionId, endPartitionId, Serde.JSON.toString)
          case Serde.PARQUET =>
            val intermediateDf = readMaterializedRawData(spark, physicalLocation, startPartitionId, endPartitionId, serde.toString)
            EncoderUtil.decodeColumnNamesInDf(intermediateDf)
          case _ =>
            throw new IllegalArgumentException("Not supported serde " + serde)
        }

      }
      catch {
        case ex: Exception =>
          logger.error(s"Error during reading data for table: $tableName", ex)
          throw DataReadException(s"Error during reading data for table: $tableName")
      }
      tableDfval
    }

    private def loadRawData(spark: SparkSession, physicalLocation: String, startPartitionId: String, endPartitionId: String, adapterClass: String, loadStrategy:ViewInputLoadStrategy, dynamicBucketPath:Option[String]): DataFrame = {

      val startDate = DateTime.parse(startPartitionId, partitionIdFormater);
      val endDate = DateTime.parse(endPartitionId, partitionIdFormater);

      //check for valid partition value
      if (!Utils.checkValidMinuteValueForRawDataPartition(startDate) || !Utils.checkValidMinuteValueForRawDataPartition(endDate)) {
        logger.error(s"Minute Value Supplied with startTime or end Time for location" +
          s" ${physicalLocation} is inValid; start: $startDate end: $endDate")
        throw new IllegalArgumentException(s"Minute Value Supplied with startTime or end Time for location" +
          s" ${physicalLocation} is inValid; start: $startDate end: $endDate")
      }
      val tableDf = try {

        val sourceAdapterClass = adapterClass
        logger.info(s"Invoking the source adapter plugin with the class - ${sourceAdapterClass}")
        val pluginInstance = Class.forName(sourceAdapterClass).newInstance().asInstanceOf[SourceAdapterPlugin]
        var finalPaths: Seq[String]  =
          if(loadStrategy == ViewInputLoadStrategy.STATIC)
          {
            Seq[String](physicalLocation)
          }
          else
          {
              Utils.getFinalPathAsList(physicalLocation, startPartitionId, endPartitionId, Consts.defaultRawDataGranularity, dynamicBucketPath);
          }
        logger.debug(s"Load strategy is ${loadStrategy} and input data paths to be read : ${finalPaths.mkString(",")}")

        addToInputPathList(finalPaths)
        pluginInstance.readData(spark, finalPaths, sourceConf)
      } catch {
        case ex: Exception =>
          logger.error(s"Error during reading data for table: $tableName", ex)
          throw DataReadException(s"Error during reading data for table: $tableName")
      }

      //Use partitions for predicate pushdown.
      //      if(serde != Serde.ASSIST_INTERACTIONS && serde != Serde.ASSIST_TRANSCRIPTS && serde != Serde.SEQ_JSON) {
      //        logger.info("Inside df filter")
      //        var x = isInRange(startDate, endDate)($"year", $"month", $"day", $"hour", $"min")
      //        tableDf = tableDf.filter(x)
      //      }
      tableDf
    }

    def addToInputPathList(finalPaths:Seq[String])=
    {
      inputPathsList ++= finalPaths
    }

    private def readMaterializedRawData(spark: SparkSession, inputPath: String, startDate: String, endDate: String, fileFormat: String): DataFrame = {
      val finalPaths: Seq[String] = inputPath.split(",")
      finalPaths.foreach((path: String) =>
        if (!CustomOutputPartitioner.isPathExist(path + "/_SUCCESS")) {
          logger.error(s"No _SUCCESS file for the path : $path")
          throw DataReadException(s"Materialization is not complete for the view : $path")
        }
      )
      spark.sqlContext.read.format(fileFormat).load(finalPaths: _*)
    }

    override def toString: String = {
      s"Table: $tableName \t @ ${location.uriFormat}  (${serde}) (dqEnabled: $dqEnable) (materializationEnabled: $materializationEnabled)" +
        (
          if (!tableBuilder.isEmpty) {
            var str: String = ""
            str = str + s"""\n\t [Builder] \t #  ${tableBuilder.get.imports.mkString(",")} """
            if (SpartanPluginTypes.withName(tableBuilder.get.tableBuilderPlugInConf.plugInConfType) == SpartanPluginTypes.SQL_PROCESSOR) {
              tableBuilder.get.tableBuilderPlugInConf.asInstanceOf[SQLTableBuilderPlugInConf].sqls.foreach(sql =>
                str = str + s"\n\t\t ${sql.outputRef} = ${sql.sql} "
              )
            } else if (SpartanPluginTypes.withName(tableBuilder.get.tableBuilderPlugInConf.plugInConfType) == SpartanPluginTypes.SCALA_PROCESSOR) {
              str = str + s"Plugin Class = ${
                tableBuilder.get.tableBuilderPlugInConf.
                  asInstanceOf[ScalaTableBuilderPlugInConf].plugInMainClass
              }"
            } else {
              str = str + "Unknown"
            }

            str
          })
    }
  }

  def pushMetricsToKafka: Unit ={
    var correlationId = ThreadContext.get(Consts.OOZIE_JOB_ID)
    var viewName = ThreadContext.get(Consts.VIEW_NAME)
    var clientName = ThreadContext.get(Consts.CLIENT_NAME)
    val DATE_FORMAT = "yyyy-MM-dd'T'HH:mmZ"
    val dateFormat = new SimpleDateFormat(DATE_FORMAT)
    val nominalTime = dateFormat.parse(ThreadContext.get(Consts.NOMINAL_TIME)).getTime()
    implicit val formats = net.liftweb.json.DefaultFormats
    val topic = PropertiesUtil.getProperty(Consts.CLIENTMETRICS_TOPIC)
    if(topic == null )
    {
      logger.info("kafka topic name not defined, hence not pushing any metrics")
    }
    else
    {
      val stats = write(PlatformDetailsStats(correlationId, PropertiesUtil.getProperty(Consts.CLIENTMETRICS_ESINDEX), System.currentTimeMillis(),
        SpartanStats(viewName,clientName,nominalTime,System.currentTimeMillis().toString,inputPathsList.toSeq)))
      logger.info("logging stats : "+stats)
      try {
        KafkaWriter.pushMetrics(topic,correlationId,stats)
      }
      catch {
        case ex: Exception => logger.error("Exception while pushing spartan metrics to kafka.", ex)
      }
    }
  }

}
case class PlatformDetailsStats(id: String, eventSource: String, eventTime: Long, body: SpartanStats)
case class SpartanStats(viewName_keyword:String, clientName_keyword:String, nominal_time:Long , processingExecution_time:String, dataPaths:Seq[String])