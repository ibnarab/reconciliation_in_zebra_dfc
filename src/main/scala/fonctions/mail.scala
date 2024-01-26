package fonctions

import javax.activation.DataHandler
import javax.mail.internet.{MimeBodyPart, MimeMultipart}
import javax.mail.util.ByteArrayDataSource
import org.apache.commons.mail.MultiPartEmail
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object mail {


  /*val partitionmonth = args(0).substring(0, 6)
  val partitionDay = args(0).trim


  println("WRITETTTTTTTTTTTTTTTTTTTTTTTTTT")
  println("*" * 50 + "usage_roaming : " + "*" * 50)
  //df_final.show(false)
  reconciliationRecharge.write
    .format("com.crealytics.spark.excel")
    .option("dataAddress", "'USAGE_ROAMING'!A1")
    .option("header", "true")
    //.mode("append")
    .mode("overwrite")
    //.save("/FileStore/my_excel_file.xlsx")
    .save(s"/dlk/osn/refined/usage_roaming_forfait.$partitionmonth.xlsx")

  /*forfaitroaming.write
    .format("com.crealytics.spark.excel")
    .option("dataAddress", "'FORFAIT_ROAMING'!A1")
    .option("header", "true")
    //.mode("append")
    .mode("append")
    //.save("/FileStore/my_excel_file.xlsx")
    .save(s"/dlk/osn/refined/usage_roaming_forfait.$partitionmonth.xlsx")*/


  val spark = SparkSession.builder
    .appName("reconciliation")
    .config("spark.hadoop.fs.defaultFS", "hdfs://bigdata")
    .config("spark.master", "yarn")
    .config("spark.submit.deployMode", "cluster")
    .enableHiveSupport()
    .getOrCreate()


  def sendMail() = {
    val multipart = new MimeMultipart()

    val messageBodyPart = new MimeBodyPart()

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    //val stream = fs.open(new Path(s"hdfs://bigdata/dlk/osn/refined/roamingMOB/roamingInOutData_$partitionDay.csv"))
    val stream = fs.open(new Path(s"hdfs://bigdata/dlk/osn/refined/usage_roaming_forfait.$partitionmonth.xlsx"))
    messageBodyPart.setDataHandler(new DataHandler(new ByteArrayDataSource(stream,"application/vnd.ms-excel")))
    //messageBodyPart.setDataHandler(new DataHandler(new ByteArrayDataSource(stream,"text/csv")))
    messageBodyPart.setFileName(s"usage_roaming_forfait.$partitionmonth.xlsx")


    multipart.addBodyPart(messageBodyPart)
    val objet = s"REPORTING USAGE ET FORFAIT ROAMING  $partitionmonth"
    println(objet)
    val  corps = s"Bonjour , \nCi-joint les reportings :\n* Usage Roaming par pays et par operateur.\n* Forfait Roaming $partitionmonth \n\nCordialement, \nL'équipe DBM"
    println(corps)

    val RECEIVER = "mariama.diouf2@orange-sonatel.com,AmadouLamine.Niang@orange-sonatel.com,aminatatabara.gueye@orange-sonatel.com,AminataMacky.Tall@orange-sonatel.com,NdeyeRokhaya.DIA@orange-sonatel.com"
    println(RECEIVER)
    val SENDER = "usage_roaming@orange-sonatel.com"
    println(SENDER)
    val email = new MultiPartEmail()
    email.setHostName("10.100.56.56")
    email.setFrom(SENDER)
    email.setContent(multipart)
    email.addPart(multipart)

    email.setSubject(objet)
    email.setMsg(corps)

    email.addTo(RECEIVER.split(",").toSeq: _*)

    email.send()

    val status = "Message envoyer avec succes a " + RECEIVER
    println(status)
  }
  sendMail()*/

}
