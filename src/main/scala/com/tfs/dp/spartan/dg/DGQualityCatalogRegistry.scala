package com.tfs.dp.spartan.dg

import scala.collection.mutable

/**
  * Created by Vijay Muvva on 29/08/2018
  */
object DGQualityCatalogRegistry {

  val dqCatalog: mutable.HashMap[String, DGTableDQDef] = new mutable.HashMap[String, DGTableDQDef]()

  def addTableDQDef(tableName: String, tableDQDef: DGTableDQDef) = {
    dqCatalog.put(tableName, tableDQDef)
  }
}
