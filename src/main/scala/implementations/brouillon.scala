package implementations

import org.apache.spark.sql.functions.{col, lit, when}

object brouillon {

  def main(args: Array[String]): Unit = {

    /*val spark = SparkSession.builder
      .appName("reconciliation")
      .config("spark.hadoop.fs.defaultFS", "hdfs://bigdata")
      .config("spark.master", "yarn")
      .config("spark.submit.deployMode", "cluster")
      .enableHiveSupport()
      .getOrCreate()


    val schemaRechargeInDetailDF = StructType(
      Array(
        StructField("msisdn"                ,     StringType  , nullable = true)  ,
        StructField("channel_name"          ,     StringType  , nullable = true)  ,
        StructField("channel_ca_recharge"   ,     StringType  , nullable = true)  ,
        StructField("type_recharge"         ,     StringType  , nullable = true)  ,
        StructField("canal_recharge"        ,     StringType  , nullable = true)  ,
        StructField("montant"               ,     LongType    , nullable = true)  ,
        StructField("segment"               ,     StringType  , nullable = true)  ,
        StructField("formule"               ,     StringType  , nullable = true)  ,
        StructField("marche"                ,     StringType  , nullable = true)  ,
        StructField("region"                ,     StringType  , nullable = true)  ,
        StructField("departement"           ,     StringType  , nullable = true)  ,
        StructField("commune_arrondissement",     StringType  , nullable = true)  ,
        StructField("zone_drv"              ,     StringType  , nullable = true)  ,
        StructField("zone_drvnew"           ,     StringType  , nullable = true)  ,
        StructField("zone_dvri"             ,     StringType  , nullable = true)  ,
        StructField("zone_dvrinew"          ,     StringType  , nullable = true)  ,
        StructField("year"                  ,     StringType  , nullable = true)  ,
        StructField("month"                 ,     StringType  , nullable = true)  ,
        StructField("day"                   ,     StringType  , nullable = true)
      )
    )

    val schemaRechargeDetaillee = StructType(
      Array(
        StructField("date_recharge"       ,     TimestampType , nullable = true)  ,
        StructField("heure_recharge"      ,     StringType    , nullable = true)  ,
        StructField("canal_recharge"      ,     StringType    , nullable = true)  ,
        StructField("rech_carte_verte"    ,     StringType    , nullable = true)  ,
        StructField("montant_recharge"    ,     DoubleType    , nullable = true)  ,
        StructField("type_recharge"       ,     StringType    , nullable = true)  ,
        StructField("type_recharge_desc"  ,     StringType    , nullable = true)  ,
        StructField("msisdn"              ,     StringType    , nullable = true)  ,
        StructField("formule"             ,     StringType    , nullable = true)  ,
        StructField("year"                ,     StringType    , nullable = true)  ,
        StructField("month"               ,     StringType    , nullable = true)  ,
        StructField("day"                 ,     StringType    , nullable = true)  ,

      )
    )
    val rechargeInDetailDF = spark.read.schema(schemaRechargeInDetailDF).parquet("/dlk/osn/refined/Recharge/recharge_in_detail/year=2023/month=12")
    val rechargeDetailleeDF = spark.read.schema(schemaRechargeDetaillee).parquet("/dlk/osn/refined/Recharge/recharge_detaillee/year=2023/month=12")


    val rechargeInDetailDFFiltre = rechargeInDetailDF.where(
      (col("canal_recharge") === "Ca Credit OM" && col("channel_name").isin("Rech_OM_O&M", "Webservices Recharge(Orange Money)", "Rech_OM_Distri"))
        ||
        (col("canal_recharge") === "CA Wave")
        ||
        (col("canal_recharge") === "Ca IAH")
        ||
        (col("canal_recharge") === "Ca Seddo")
        ||
        (col("canal_recharge") === "Ca Cartes")

    )


    val rechargeDetailleeDFFiltre = rechargeDetailleeDF.where(
      (col("type_recharge") === "M")
        ||
        (col("type_recharge") === "O")
        ||
        (col("type_recharge") === "D")
        ||
        (col("type_recharge") === "W")
        ||
        (col("type_recharge") === "V")
        ||
        (col("type_recharge") === "A")
        ||
        (col("type_recharge") === "E")
        ||
        (col("type_recharge") === "C")

    )


    val rechargeInDetailDFFiltreFinal = rechargeInDetailDFFiltre
      .withColumnRenamed("day", "date")
      .withColumnRenamed("msisdn", "numero")
      .withColumn("type_recharge",
        when(col("canal_recharge")  === "Ca Credit OM" && col("channel_name") === "Rech_OM_O&M", lit("Recharge Orange Money O&M"))
          .when(col("canal_recharge") === "Ca Credit OM" && col("channel_name") === "Webservices Recharge(Orange Money)", lit("Recharge Orange Money S2S"))
          .when(col("canal_recharge") === "Ca Credit OM" && col("channel_name") === "Rech_OM_Distri", lit("Recharge Orange Money Distri"))
          .when(col("canal_recharge") === "CA Wave", lit("Recharge Wave"))
          .when(col("canal_recharge") === "Ca IAH", lit("Recharge IAH"))
          .when(col("canal_recharge") === "Ca Seddo" && col("channel_name") === "Corporate Recharge", lit("Seddo Corporate"))
          .when(col("canal_recharge") === "Ca Seddo" && col("channel_name").isin("MobileSelfService", "IT Subscription", "RMS Charging", "Webservices Recharge(C2S)"), lit("Recharge C2S"))
          .when(col("canal_recharge") === "Ca Cartes", lit("Recharge Carte"))
          .otherwise(lit("Pas de recharge"))
      )
      .withColumn("montant", col("montant").cast("double"))
      .withColumn("source_in_zebra", lit("IN"))
      .drop("channel_name", "channel_ca_recharge", "canal_recharge", "segment", "marche", "region", "departement", "commune_arrondissement", "zone_drv", "zone_drvnew", "zone_dvri", "zone_dvrinew", "year", "month")
      .select("date", "numero", "type_recharge", "formule", "montant", "source_in_zebra")


    val rechargeDetailleeDFFiltreFinal = rechargeDetailleeDFFiltre
      .withColumnRenamed("day", "date")
      .withColumnRenamed("msisdn", "numero")
      .withColumn("type_recharge_detaillee",
        when(col("type_recharge") === "M", lit("Recharge Orange Money O&M"))
          .when(col("type_recharge") === "O", lit("Recharge Orange Money S2S"))
          .when(col("type_recharge") === "D", lit("Recharge Orange Money Distri"))
          .when(col("type_recharge") === "W", lit("Recharge Wave"))
          .when(col("type_recharge") === "V", lit("Recharge IAH"))
          .when(col("type_recharge") === "A", lit("Seddo Corporate"))
          .when(col("type_recharge") === "E", lit("Recharge C2S"))
          .when(col("type_recharge") === "C", lit("Recharge Carte"))
          .otherwise(lit("Pas de recharge"))
      )
      .withColumnRenamed("montant_recharge", "montant")
      .withColumn("source_in_zebra", lit("ZEBRA"))
      .drop("date_recharge", "heure_recharge", "canal_recharge", "rech_carte_verte", "type_recharge_desc", "year", "month", "type_recharge")
      .select("date", "numero", "type_recharge_detaillee", "formule", "montant", "source_in_zebra")


    val rechargeFinal = rechargeInDetailDFFiltreFinal.union(rechargeDetailleeDFFiltreFinal)

    rechargeInDetailDFFiltreFinal.printSchema()
    rechargeDetailleeDFFiltreFinal.printSchema()
    rechargeFinal.printSchema()

    rechargeInDetailDFFiltreFinal.show(100, false)
    rechargeDetailleeDFFiltreFinal.show(100, false)
    rechargeFinal.show(100, false)*/

  }

}
