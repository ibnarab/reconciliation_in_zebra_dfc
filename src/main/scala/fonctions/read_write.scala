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

  def writeHiveInZebraGeneral(dataFrame: DataFrame) : Unit =  {

    dataFrame.write
      .mode(SaveMode.Overwrite)
      .option("path", "dfc_temp.reconciiation_in_zebra_"+schema_chemin_hdfs.annee()+"_"+schema_chemin_hdfs.moisPrecedent())
      .saveAsTable("dfc_temp.reconciiation_in_zebra_"+schema_chemin_hdfs.annee()+"_"+schema_chemin_hdfs.moisPrecedent())
  }

  def writeHiveInZebraAgr(dataFrame: DataFrame) : Unit =  {

    dataFrame.write
      .mode(SaveMode.Overwrite)
      .option("path", "dfc_temp.in_zebra_agr_"+schema_chemin_hdfs.annee()+"_"+schema_chemin_hdfs.moisPrecedent())
      .saveAsTable("in_zebra_agr_"+schema_chemin_hdfs.annee()+"_"+schema_chemin_hdfs.moisPrecedent())
  }

}
