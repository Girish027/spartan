package com.tfs.dp.configuration.service

import com.tfs.dp.spartan.dg.DGCatalogModel.PhysicalTable
import com.tfs.dp.spartan.dg.DGTableDQDef

/**
 * Created by devinder.k on 12/5/2017.
 */
trait CatalogService {
  def getViewDefinition(viewName: String, clientName: String, scheduleStartTime: String, scheduleEndTime:String, finalView: String, isDevMode: Boolean=false): PhysicalTable
  def getDQDef(viewName: String):DGTableDQDef
}
