package com.tfs.dp.spartan.dg

import com.tfs.dp.spartan.conf.Consts
import com.tfs.dp.spartan.dg.DGCatalogModel._

import scala.collection.mutable

/**
  * Created by guruprasad.gv on 8/26/17.
  */
object DGCatalogRegistry {

  // Commenting the Hardcoded Table Definitions
  /*val rawIdm: PhysicalTable =
    PhysicalTable(
      "RawIDM",
      Location("/raw/prod/rtdp/idm/events/%s/"),
      Serde.AVRO,
      Map("ebay" -> "eBay"),
      deduplication = RawEventDeduplicator(Seq("header.eventUUID"))
    );

  val rawAssistInteractions: PhysicalTable =
    PhysicalTable(
      "RawAssistInteractions",
      Location("/raw/prod/%s/pxassist/interactions/"),
      Serde.ASSIST_INTERACTIONS,
      Map.empty,
      deduplication = RawEventDeduplicator(Seq("context"))
    );

  val rawAssistTranscripts: PhysicalTable =
    PhysicalTable(
      "RawAssistTranscripts",
      Location("/raw/prod/%s/pxassist/transcripts/"),
      Serde.ASSIST_TRANSCRIPTS,
      Map.empty,
      deduplication = RawEventDeduplicator(Seq("message"))
    );

  val resolutionReport: PhysicalTable =

    PhysicalTable(
      "ResolutionReport",
      Location("/reports/ResolutionReport"),
      Serde.JSON,
      Map.empty,
      Option(TableBuilder(
        Seq(
          Import(rawIdm, null, null)
        ),
        Seq(
          SQL("ResolutionReport", "select header.timeEpochMillisUTC as timeEpochMillisUTC, header.clientId, header.applicationId, header.channel, header.eventType, header.associativeTag, header.channelSessionId, header.environment from RawIDM")
        )))
    )*/


  val registry: mutable.HashMap[String, PhysicalTable] =

    mutable.HashMap(
      //Commenting the hardcoded Table Definitions
     /* "RawIDM" -> rawIdm,
      "RawAssistInteractions" -> rawAssistInteractions,
      "RawAssistTranscripts" -> rawAssistTranscripts,
      "ResolutionReport" -> resolutionReport*/
    )

  /**
    * Add a new Table to catalog
    *
    * @param tableName
    * @param location
    * @param serde
    *
    */
  def add(tableName: String, location: String,
          serde: Serde.Value, clientNameOverrides: Map[String, String]): Unit = {

    registry.put(tableName, PhysicalTable(
      tableName,
      Location(location),
      serde,
      clientNameOverrides
    ))
  }

  /**
    * Add a new Table to catalog
    *
    * @param tableName
    * @param location
    * @param serde
    * @param imports
    * @param sqls
    */
  def add(tableName: String, location: String,
          serde: Serde.Value,
          imports: Seq[Import], sqls: Seq[SQL],
          clientNameOverrides: Map[String, String],
          sourceConfList:Map[String,String]) = {

    registry.put(tableName, PhysicalTable(
      tableName,
      Location(location),
      serde,
      Map.empty,
      sourceConfList,
      Some(Consts.GENERIC_SOURCE_PLUGIN_MAIN_CLASS),
      Option(
        TableBuilder(
          imports, SQLTableBuilderPlugInConf(SpartanPluginTypes.SQL_PROCESSOR.toString,
            Consts.GENERIC_SPARK_SQL_PLUGIN_MAIN_CLASS, sqls)
        )
      )
    ));

  }

}
