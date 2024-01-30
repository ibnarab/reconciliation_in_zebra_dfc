package implementations

import fonctions.{read_write, schema_chemin_hdfs, utils, constants}


import implementations.reconciliation_in_zebra
import fonctions.{read_write, schema_chemin_hdfs}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.commons.mail.MultiPartEmail
import org.apache.hadoop.fs.{FileSystem, Path}
import javax.activation.DataHandler
import javax.mail.internet.{MimeBodyPart, MimeMultipart}
import javax.mail.util.ByteArrayDataSource


object reconciliation_in_zebra {

  def main(args: Array[String]): Unit = {



    val rechargeInDetail        = read_write.readParquet_in_zebra(false, schema_chemin_hdfs.chemin_read_in_detail, schema_chemin_hdfs.schemaRechargeInDetailDF)
    val rechargeDetaillee       = read_write.readParquet_in_zebra(false, schema_chemin_hdfs.chemin_read_detaillee, schema_chemin_hdfs.schemaRechargeDetaillee)

    val rechargeInDetailFiltre  = utils.filtre_recharge_in_detail(rechargeInDetail)
    val rechargeDetailleeFiltre = utils.filtre_recharge_detaillee(rechargeDetaillee)


    val inDetailAddRenameColumns = utils.add_columns_and_rename_detail_in(rechargeInDetailFiltre)
    val detailleeAddRenameColumns = utils.add_columns_and_rename_detaillee(rechargeDetailleeFiltre)

    val uniqueRowsWithoutSourceInDetail = inDetailAddRenameColumns.except(detailleeAddRenameColumns)
    val uniqueRowsWithoutSourceDetaillee = detailleeAddRenameColumns.except(inDetailAddRenameColumns)


    val uniqueRowsWithSourceInDetail = utils.unique_rows_with_source(uniqueRowsWithoutSourceInDetail, "IN")
    val uniqueRowsWithSourceDetaillee = utils.unique_rows_with_source(uniqueRowsWithoutSourceDetaillee, "ZEBRA")




    val reconciliationRecharge = uniqueRowsWithSourceInDetail.union(uniqueRowsWithSourceDetaillee)
    val reconciliationAggregee = utils.reconciliation_agregee(uniqueRowsWithoutSourceInDetail, uniqueRowsWithoutSourceDetaillee)



    read_write.writeHiveInZebra(reconciliationRecharge, true, schema_chemin_hdfs.chemin_write_in_detail, schema_chemin_hdfs.table_write_in_detail)
    read_write.writeHiveInZebra(reconciliationAggregee, true, schema_chemin_hdfs.chemin_write_detaillee, schema_chemin_hdfs.table_write_detaillee)


    /*read_write.writeHiveInZebraGeneral(reconciliationRecharge)
    read_write.writeHiveInZebraAgr(reconciliationAggregee)*/





/*                        Mail                                                                                                  */




    val partitionmonth = args(0).substring(0, 6)
    val partitionDay = args(0).trim


    println("WRITETTTTTTTTTTTTTTTTTTTTTTTTTT")
    println("*" * 50 + "usage_roaming : " + "*" * 50)
    //df_final.show(false)
    reconciliationAggregee.write
      .format("com.crealytics.spark.excel")
      .option("dataAddress", "Reconciliation recharge in_zebra")
      .option("header", "true")
      //.mode("append")
      .mode("overwrite")
      //.save("/FileStore/my_excel_file.xlsx")
      .save(s"/dlk/osn/refined/reconciliation_recharge_in_zebra.$partitionmonth.xlsx")


    def sendMail() = {
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
    }
    sendMail()






/*                                            TEST                                                                                                   */

    // Gerer les espaces blancs

    reconciliationRecharge.printSchema()
    reconciliationAggregee.printSchema()

    /*uniqueRowsWithSourceInDetail.show(100, false)
    uniqueRowsWithSourceDetaillee.show(100, false)*/

    reconciliationAggregee.show(100, false)





  }

}
