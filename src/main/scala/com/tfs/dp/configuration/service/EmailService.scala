package com.tfs.dp.configuration.service

import java.util.Properties

import com.tfs.dp.exceptions.EmailServiceException
import com.tfs.dp.spartan.conf.Consts
import com.tfs.dp.spartan.utils.PropertiesUtil
import javax.mail.{Message, Session, Transport}
import javax.mail.internet.{InternetAddress, MimeMessage}
import org.apache.logging.log4j.scala.Logging

object EmailService extends Logging {

  def send(email:Email) = {

    logger.info(s"Sending an email: ${email.toString}")

    try {
      val mailSession:Session = Session.getInstance(getProps)
      val message = new MimeMessage(mailSession)
      message.setFrom(new InternetAddress(email.from))
      message.setRecipient(Message.RecipientType.TO, new InternetAddress(email.to))
      message.setSubject(email.subject)
      message.setContent(email.msg, "text/html;charset=UTF-8")
      Transport.send(message)
      logger.info("Email sent successfully..")
    } catch {
      case ex:Exception =>
        logger.error(s"Error sending an email: ${email.toString}")
        throw EmailServiceException(s"Error sending an email: ${email.toString}")
    }

  }

  private def getProps:Properties = {
    val props:Properties = new Properties()
    props.put(Consts.MAIL_SMTP_HOST, PropertiesUtil.getProperty(Consts.MAIL_SMTP_HOST))
    props.put(Consts.MAIL_SMTP_PORT, PropertiesUtil.getProperty(Consts.MAIL_SMTP_PORT))
    props.put(Consts.MAIL_SMTP_AUTH, PropertiesUtil.getProperty(Consts.MAIL_SMTP_AUTH))
    props.put(Consts.MAIL_SMTP_START_TLS_ENABLE, PropertiesUtil.getProperty(Consts.MAIL_SMTP_START_TLS_ENABLE))
    props
  }

  def main(args: Array[String]): Unit = {


    val dqMsg =
      """<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtm
        |l1-transitional.dtd"><html xmlns="http://www.w3.org/1999/xhtml"><head> <meta name="viewport" content=
        |"width=device-width"/> <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/> <title>DQ
        |Alert</title> <style type="text/css"> .body-wrap,body{background-color:#f6f6f6}.footer,.footer a{color
        |:#999}.aligncenter,.btn-primary{text-align:center}*{margin:0;padding:0;font-family:"Helvetica Neue",
        |Helvetica,Helvetica,Arial,sans-serif;box-sizing:border-box;font-size:14px}.content,.content-wrap
        |{padding:20px}img{max-width:100%}body{-webkit-font-smoothing:antialiased;-webkit-text-size-adjust:none;width:
        |100%!important;height:100%;line-height:1.6}table td{vertical-align:top}.body-wrap{width:100%}.container
        |{display:block!important;max-width:600px!important;margin:0 auto!important;clear:both!important}.clear,
        |.footer{clear:both}.content{margin:0 auto;display:block}.main{background:#fff;border:1px solid #e9e9e9;
        |border-radius:3px}.content-block{padding:0 0 20px}.header{width:100%;margin-bottom:20px}.footer{width:
        |100%;padding:20px}.footer a,.footer p,.footer td,.footer unsubscribe{font-size:12px}h1,h2,h3
        |{font-family:"Helvetica Neue",Helvetica,Arial,"Lucida Grande",sans-serif;color:#000;margin:40px
        |0 0;line-height:1.2;font-weight:400}h1{font-size:32px;font-weight:500}h2{font-size:24px}h3{font-size:18px}
        |h4{font-size:14px;font-weight:600}ol,p,ul{margin-bottom:10px;font-weight:400}ol li,p li,ul li{margin-left:5px;
        |list-style-position:inside}a{color:#ee8720;text-decoration:underline}.alert a,.btn-primary{text-decoration:
        |none}.btn-primary{color:#FFF;background-color:#ee8720;border:solid #ee8720;border-width:5px 10px;line-height:
        |2;font-weight:700;cursor:pointer;display:inline-block;border-radius:5px;text-transform:capitalize}.alert,.
        |alert a{color:#fff;font-weight:500;font-size:16px}.last{margin-bottom:0}.first{margin-top:0}.alignright{text-
        |align:right}.alignleft{text-align:left}.alert{padding:20px;text-align:center;border-radius:3px 3px 0 0}.alert.
        |alert-warning{background:#f8ac59}.alert.alert-bad{background:#ed5565}.alert.alert-good{background:#ee8720}.invoice{margin:40px auto;text-align:left;width:80%}.invoice td{padding:5px 0}.invoice .invoice-items{width:100%}.invoice .invoice-items td{border-top:#eee 1px solid}.invoice .invoice-items .total td{border-top:2px solid #333;border-bottom:2px solid #333;font-weight:700}@media only screen and (max-width:640px){.container,.invoice{width:100%!important}h1,h2,h3,h4{font-weight:600!important;margin:20px 0 5px!important}h1{font-size:22px!important}h2{font-size:18px!important}h3{font-size:16px!important}.content,.content-wrap{padding:10px!important}}</style></head><body><table class="body-wrap"> <tr> <td></td><td width="600"> <br><table class="main" width="100%" cellpadding="0" cellspacing="0"> <tr> <td class="alert alert-good"> A DQ violation has been detected..! </td></tr><tr> <td class="content-wrap"> <table width="100%" cellpadding="0" cellspacing="0"> <tr> <td class="content-block"> <table style="border-collapse: collapse;"> <tbody> <tr> <td style="border: 1px solid orange;padding: 10px;text-align: left;"> <strong>View&nbsp;Name:</strong> </td><td style="border: 1px solid orange;padding:9px;text-align: left;"> View_DCF </td></tr><tr> <td style="border: 1px solid orange;padding: 10px;text-align: left;"> <strong>Client:</strong> </td><td style="border: 1px solid orange;padding:9px;text-align: left;"> dish </td></tr><tr> <td style="border: 1px solid orange;padding: 10px;text-align: left;"> <strong>DQ&nbsp;Data&nbsp;Set&nbsp;Name:</strong> </td><td style="border: 1px solid orange;padding:9px;text-align: left;"> DCF_Output_Data_Set </td></tr><tr> <td style="border: 1px solid orange;padding: 10px;text-align: left;"> <strong>Time&nbsp;Range:</strong> </td><td style="border: 1px solid orange;padding:9px;text-align: left;"> 2018-01-01 01:00 - 2018-01-02 02:00 </td></tr><tr> <td style="border: 1px solid orange;padding: 10px;text-align: left;"> <strong>Evaluation&nbsp;Time:</strong> </td><td style="border: 1px solid orange;padding:9px;text-align: left;"> 2018-01-01 01:00 </td></tr><tr> <td style="border: 1px solid orange;padding: 10px;text-align: left;"> <strong>Data Set SQL:</strong> </td><td style="border: 1px solid orange;padding:9px;text-align: left;"> select * from View_DCF </td></tr><tr> <td style="border: 1px solid orange;padding: 10px;text-align: left;"> <strong>DQ Rules:</strong> </td><td style="border: 1px solid orange;padding:9px;text-align: left;"> lower(UserAgent)!='null' and length(trim(UserAgent))>=1, <br>inviteResponseTimeEpochMillisUTC<=chatStartedTimeEpochMillisUTC, inviteResponseTimeEpochMillisUTC<=chatStartedTimeEpochMillisUTC </td></tr></tbody></table> </td></tr><tr> <td class="content-block"> <a href="#" class="btn-primary">View detailed report</a> </td></tr><tr> <td class="content-block"> <i>This is an automated alert generated by the data platform systems.</i> </td></tr></table> </td></tr></table> </td><td></td></tr></table></body></html>""".stripMargin

    val props:Properties = new Properties()
    props.put("mail.smtp.host", "localhost")
    props.put("mail.smtp.port", "2525")
    props.put("mail.smtp.auth", "false")
    props.put("mail.smtp.starttls.enable", "true")

    val mailSession:Session = Session.getInstance(props)
    val message = new MimeMessage(mailSession)
    message.setFrom(new InternetAddress("dataplat@247.ai"))
    message.setRecipient(Message.RecipientType.TO, new InternetAddress("p7b6l7x6p0q4h8i9@247-inc.slack.com"))
    message.setSubject("DQ FAILURE:")
    message.setContent(dqMsg, "text/html;charset=UTF-8")
    Transport.send(message)
  }
}

case class Email(from:String, to:String, subject:String, msg:String)
