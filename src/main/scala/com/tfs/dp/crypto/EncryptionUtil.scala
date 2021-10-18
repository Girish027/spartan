package com.tfs.dp.crypto

import java.util.Base64

import com.tfs.dp.spartan.conf.Consts
import com.tfs.dp.spartan.dg.DGCatalogModel.TableColumn
import com.tfsc.ilabs.crypto.decryption.IDecryptionService
import com.tfsc.ilabs.crypto.dto.{ClientDetails, CryptoProperties}
import com.tfsc.ilabs.crypto.encryption.service.IEncryptionService
import com.tfsc.ilabs.crypto.service.CryptoService
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

import com.tfsc.ilabs.crypto.consts.Constants

import scala.util.Random

object EncryptionUtil extends Logging {

  private var encryptionService: IEncryptionService = _
  private var decryptionService: IDecryptionService = _

  init()
  
  def init(clientName: String = Constants.DEFAULT_BC_CLIENT_ID, accountId: String = Constants.DEFAULT_BC_ACCOUNT_ID,
           isValutEnabled: Boolean = false, isBouncyCastleEnabled: Boolean = true): Unit = {
    val cryptoProperties = CryptoProperties.builder.isBouncyCastleEnabled(isBouncyCastleEnabled).isVaultEnabled(isValutEnabled).build
    val clientDetails = ClientDetails.builder.clientId(clientName).accountId(accountId).build
    val cryptoContext = CryptoService.getInstance.initializeContext(cryptoProperties)
    encryptionService = CryptoService.getInstance.getEncryptionService(clientDetails, cryptoContext)
    decryptionService = CryptoService.getInstance.getDecryptionService(cryptoContext)
  }

  def encrypt[A](plainText: A): Option[String] = {
    Option(plainText) match {
      case Some(_) =>
        Option(Base64.getEncoder.encodeToString(encryptionService.encryptBytes(plainText.toString.getBytes(
          Constants.DEFAULT_CHAR_SET))))
      case None => None
    }
  }

  def decrypt(cipherText: String): Option[String] = {
    Option(cipherText) match {
      case Some(_) => Option(new String(decryptionService.decryptBytes(Base64.getDecoder.decode(cipherText))))
      case None => None
    }
  }

  def getSensitiveColNames(cols: Seq[TableColumn]): Seq[String] = {
    Option(cols) match {
      case Some(cols) => cols.filter(_.isSensitive).map(col => col.name)
      case None => Seq()
    }
  }

  def encryptCols(sparkSession: SparkSession, df: DataFrame, cols: Seq[TableColumn]): DataFrame = {
    val colNames = getSensitiveColNames(cols)
    colNames.isEmpty match {
      case true => df
      case false =>
        logger.info(s"The following columns will be encrypted - ${colNames.mkString(Consts.SEPARATOR_COMMA)}")
        val tableName = Random.alphanumeric.take(Consts.INTEGER_LITERAL_TEN).mkString
        var sqlQry = Consts.SQL_SELECT_ALL
        for (colName <- colNames) {
          sqlQry += Consts.SEPARATOR_COMMA + Consts.SPACE + Consts.ENCRYPT_UDF_NAME + Consts.OPENING_BRACKET +
            colName + Consts.CLOSING_BRACKET + Consts.SPACE + Consts.SQL_AS + Consts.SPACE + Consts.BACK_QUOTE +
            colName + Consts.ENCRYPTED_SUFFIX + Consts.BACK_QUOTE
        }
        sqlQry += Consts.SPACE + Consts.SQL_FROM + Consts.SPACE + tableName
        logger.info(s"Encryption SQL - $sqlQry")
        df.createOrReplaceTempView(tableName)
        var dfEncrypted = sparkSession.sql(sqlQry)
        dfEncrypted = dfEncrypted.drop(colNames: _*)
        for (colName <- colNames) {
          dfEncrypted = dfEncrypted.withColumnRenamed(colName + Consts.ENCRYPTED_SUFFIX, colName)
        }
        dfEncrypted
    }
  }

  def decryptCols(sparkSession: SparkSession, df: DataFrame, cols: Seq[TableColumn]): DataFrame = {
    val colNames = getSensitiveColNames(cols)
    colNames.isEmpty match {
      case true => df
      case false =>
        logger.info(s"The following columns will be decrypted - ${colNames.mkString(Consts.SEPARATOR_COMMA)}")
        val tableName = Random.alphanumeric.take(Consts.INTEGER_LITERAL_TEN).mkString
        var sqlQry = Consts.SQL_SELECT_ALL
        for (colName <- colNames) {
          sqlQry += Consts.SEPARATOR_COMMA + Consts.SPACE + Consts.DECRYPT_UDF_NAME + Consts.OPENING_BRACKET +
            colName + Consts.CLOSING_BRACKET + Consts.SPACE + Consts.SQL_AS + Consts.SPACE + Consts.BACK_QUOTE +
            colName + Consts.DECRYPTED_SUFFIX + Consts.BACK_QUOTE
        }
        sqlQry += Consts.SPACE + Consts.SQL_FROM + Consts.SPACE + tableName
        logger.info(s"Decryption SQL - $sqlQry")
        df.createOrReplaceTempView(tableName)
        var dfDecrypted = sparkSession.sql(sqlQry)
        dfDecrypted = dfDecrypted.drop(colNames: _*)
        for (colName <- colNames) {
          dfDecrypted = dfDecrypted.withColumnRenamed(colName + Consts.DECRYPTED_SUFFIX, colName)
        }
        dfDecrypted
    }
  }


}