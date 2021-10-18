package com.tfs.dp.spartan.dg

import scala.collection.mutable

/**
  * Created by guruprasad.gv on 9/25/17.
  */
object DGQualityCatalog {


  val genericDefinitionRegistry: mutable.HashMap[String, DGQualityDefinition] =
    new mutable.HashMap[String, DGQualityDefinition]()

  /**
    * //TODO : add client specific overrides
    *
    * @param tableName
    * @param client
    * @return
    */
  def getDQDefinition(tableName: String, client: String): DGQualityDefinition = {

    val dqDefin = genericDefinitionRegistry.get(tableName);
    var qualityDefn: DGQualityDefinition = null
    if (dqDefin.isEmpty) {
      qualityDefn = new DGQualityDefinition(new TableDQ(tableName, Seq(), Seq()))
    }
    qualityDefn = dqDefin.get
    qualityDefn
  }

  def addTableDQ(tableName: String, dGQualityDefinition: DGQualityDefinition) = {
    genericDefinitionRegistry.put(tableName, dGQualityDefinition)
  }
}
