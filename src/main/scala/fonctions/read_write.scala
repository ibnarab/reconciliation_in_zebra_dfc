package fonctions
import constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

object read_write {

  def readParquet_in_zebra(header: Boolean, chemin: String, schema: StructType) : DataFrame =  {

    spark.read
      .schema(schema)
      .format("parquet")
      .option("header", s"$header")
      .load(s"$chemin")
  }

}
