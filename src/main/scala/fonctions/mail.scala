package fonctions


/*
//import java.nio.file.FileSystem
import implementations.reconciliation_in_zebra
import fonctions.{read_write, schema_chemin_hdfs}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.commons.mail.MultiPartEmail
import org.apache.hadoop.fs.{FileSystem, Path}
import javax.activation.DataHandler
import javax.mail.internet.{MimeBodyPart, MimeMultipart}
import javax.mail.util.ByteArrayDataSource


*/

object mail {






  /*def sendMail() = {

    val partitionmonth = args(0).substring(0, 6)
    val partitionDay = args(0).trim

    val multipart = new MimeMultipart()

    val messageBodyPart = new MimeBodyPart()

    val fs = FileSystem.get(constants.spark.sparkContext.hadoopConfiguration)
    //val stream = fs.open(new Path(s"hdfs://bigdata/dlk/osn/refined/roamingMOB/roamingInOutData_$partitionDay.csv"))
    val stream = fs.open(new Path(s"hdfs://bigdata/dlk/osn/refined/reconciliation_recharge_in_zebra.$partitionmonth.xlsx"))
    messageBodyPart.setDataHandler(new DataHandler(new ByteArrayDataSource(stream,"application/vnd.ms-excel")))
    //messageBodyPart.setDataHandler(new DataHandler(new ByteArrayDataSource(stream,"text/csv")))
    messageBodyPart.setFileName(s"reconciliation_recharge_in_zebra.$partitionmonth.xlsx")


    multipart.addBodyPart(messageBodyPart)
    val objet = s"RECONCILIATION RECHARGE IN ZEBRA  $partitionmonth"
    println(objet)
    val  corps = s"Bonjour , \nCi-joint les reportings :\n* Reconciliation Recharge par jour et par type de recharge.\n* reconciliation_recharge_in_zebra $partitionmonth \n\nCordialement, \nL'équipe DBM"
    println(corps)

    val RECEIVER = "mouhamedibnarab.diop@orange-sonatel.com"
    println(RECEIVER)
    val SENDER = "reconciliation_recharge_in_zebrag@orange-sonatel.com"
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
  }*/

}
