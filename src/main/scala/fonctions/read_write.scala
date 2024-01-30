package fonctions
import constants._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.types.StructType
object read_write {

  def readParquet_in_zebra(header: Boolean, chemin: String, schema: StructType) : DataFrame =  {

    spark.read
      .schema(schema)
      .format("parquet")
      .option("header", s"$header")
      .load(s"$chemin")
  }

  /*def writeHiveInZebra(dataFrame: DataFrame, header: Boolean, chemin: String, table: String) : Unit =  {

    dataFrame.write
      .mode(SaveMode.Overwrite)
      .option("header", header)
      .option("path", chemin)
      .saveAsTable(table)
  }*/

  def writeHiveInZebra(dataFrame: DataFrame, header: Boolean, chemin: String, table: String) : Unit =  {
    dataFrame.write
      .mode(SaveMode.Overwrite)
      .partitionBy("year", "month", "day") // Spécifier les colonnes de partition
      .option("header", header)
      .option("path", chemin)
      .saveAsTable(table)
  }

  /*def writeHiveInZebraAgr(dataFrame: DataFrame) : Unit =  {

    dataFrame.write
      .mode(SaveMode.Overwrite)
      .option("path", "dfc_temp.recharge_in_zebra_agr")
      .saveAsTable("dfc_temp.recharge_in_zebra_agr_")
  }*/

}
