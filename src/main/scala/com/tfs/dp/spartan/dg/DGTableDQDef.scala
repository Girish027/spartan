package com.tfs.dp.spartan.dg

import org.apache.spark.sql.DataFrame

/**
  * Created by Vijay Muvva on 29/08/2018
  */
class DGTableDQDef(dqDataSets:Seq[DQDataSet]) {

  def getDQDataSets(): Seq[DQDataSet] = {
    dqDataSets
  }
}

case class DQDataSet(id:String, name:String, sql:String, recordIdentifier:String, description:String, level:String, dqRules:Seq[DQRule])

case class DQRule(id:String, name:String, sql:String, message:String)

case class DQResult(dqDF:DataFrame, dqDataSets:Seq[DQDataSet], errorCount:Long, dqEvalTime:String)
