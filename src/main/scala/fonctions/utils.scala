package fonctions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object utils {

    def filtre_recharge_in_detail(df: DataFrame) : DataFrame =  {

        df.where(
              (col("canal_recharge") === "Ca Credit OM" && col("channel_name")
                .isin("Rech_OM_O&M", "Webservices Recharge(Orange Money)", "Rech_OM_Distri"))
                                                                            ||
              (col("canal_recharge") === "CA Wave")
                                                                            ||
              (col("canal_recharge") === "Ca IAH")
                                                                            ||
              (col("canal_recharge") === "Ca Seddo")
                                                                            ||
              (col("canal_recharge") === "Ca Cartes")

        )
    }


    def filtre_recharge_detaillee(df: DataFrame) : DataFrame =  {

      df.where(
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
    }


    def add_columns_and_rename_detail_in(df: DataFrame) : DataFrame =  {

      df
        .withColumnRenamed("day"             ,   "date")
        .withColumnRenamed("msisdn"          , "numero")
        .withColumn(          "type_recharge"   ,
             when(col("canal_recharge")  === "Ca Credit OM" && col("channel_name") === "Rech_OM_O&M"                                                                             , lit("Recharge Orange Money O&M"))
            .when(col("canal_recharge")  === "Ca Credit OM" && col("channel_name") === "Webservices Recharge(Orange Money)"                                                      , lit("Recharge Orange Money S2S"))
            .when(col("canal_recharge")  === "Ca Credit OM" && col("channel_name") === "Rech_OM_Distri"                                                                          , lit("Recharge Orange Money Distri"))
            .when(col("canal_recharge")  === "CA Wave"                                                                                                                                     , lit("Recharge Wave"))
            .when(col("canal_recharge")  === "Ca IAH"                                                                                                                                      , lit("Recharge IAH"))
            .when(col("canal_recharge")  === "Ca Seddo"     && col("channel_name") === "Corporate Recharge"                                                                      , lit("Seddo Corporate"))
            .when(col("canal_recharge")  === "Ca Seddo"     && col("channel_name").isin("MobileSelfService", "IT Subscription", "RMS Charging", "Webservices Recharge(C2S)"), lit("Recharge C2S"))
            .when(col("canal_recharge")  === "Ca Cartes"                                                                                                                                   , lit("Recharge Carte"))
            .otherwise(                                                                                                                                                                                lit( null))
        )
        .withColumn("montant"                                                                                                                                                              , col("montant").cast("double"))
        .drop(
          "channel_name"                ,
                    "channel_ca_recharge"         ,
                    "canal_recharge"              ,
                    "segment"                     ,
                    "marche"                      ,
                    "region"                      ,
                    "departement"                 ,
                    "commune_arrondissement"      ,
                    "zone_drv"                    ,
                    "zone_drvnew"                 ,
                    "zone_dvri"                   ,
                    "zone_dvrinew"                ,
                    "year"                        ,
                    "month"
        )
        .select(
               "date"                        ,
              "numero"                      ,
                    "type_recharge"               ,
                    "formule"                     ,
                    "montant"
        )
    }


    def add_columns_and_rename_detaillee(df: DataFrame) : DataFrame =  {

      df
        .withColumnRenamed("day"                                , "date")
        .withColumnRenamed("msisdn"                             , "numero")
        .withColumn("type_recharge"                                ,
          when(col("type_recharge")             === "M"            , lit("Recharge Orange Money O&M"))
            .when(col("type_recharge")          === "O"            , lit("Recharge Orange Money S2S"))
            .when(col("type_recharge")          === "D"            , lit("Recharge Orange Money Distri"))
            .when(col("type_recharge")          === "W"            , lit("Recharge Wave"))
            .when(col("type_recharge")          === "V"            , lit("Recharge IAH"))
            .when(col("type_recharge")          === "A"            , lit("Seddo Corporate"))
            .when(col("type_recharge")          === "E"            , lit("Recharge C2S"))
            .when(col("type_recharge")          === "C"            , lit("Recharge Carte"))
            .otherwise(                                                        lit( null))
        )
        .withColumn("formule"                                      ,
          regexp_replace(
            col("formule")                                         ,
                "^(HYBRIDE-|POSTPAID-|PREPAID-)"                    ,
                ""
          )
        )
        .withColumnRenamed("montant_recharge"                   ,  "montant")
        .drop(
          "date_recharge"                                          ,
          "heure_recharge"                                                   ,
          "canal_recharge"                                                   ,
          "rech_carte_verte"                                                 ,
          "type_recharge_desc"                                               ,
          "year"                                                             ,
          "month"                                                            ,
        )
        .select(
           "date"                                                       ,
          "numero"                                                     ,
                "type_recharge"                                              ,
                "formule"                                                    ,
                "montant"                                                    ,
        )
    }



  def unique_rows_with_source(dataFrame: DataFrame, valeur: String): DataFrame = {

      dataFrame.withColumn("source_in_zebra", lit(valeur))
  }


  def agg_date_type_recharge(dataFrame: DataFrame, alias: String): DataFrame = {

    // Convertir la colonne 'date' en format date
    val dfConverted = dataFrame.withColumn("date", to_date(col("date"), "yyyyMMdd"))

    // Grouper les données et effectuer les agrégations
    val groupedDF = dfConverted.groupBy("date", "type_recharge")
      .agg(
        count("*").as(alias + "_cnt"),
        sum("montant").as(alias + "_mnt")
      )

    // Remplacer les valeurs nulles par 0 dans les colonnes spécifiques
    val filledDF = groupedDF
      .withColumn(alias + "_cnt", coalesce(col(alias + "_cnt"), lit(0)))
      .withColumn(alias + "_mnt", coalesce(col(alias + "_mnt"), lit(0)))

    // Trier le DataFrame par la colonne 'date'
    val sortedDF = filledDF.orderBy("date")

    // Sélectionner les colonnes requises
    sortedDF.select("date", "type_recharge", alias + "_cnt", alias + "_mnt")

  }


  def reconciliation_agregee(dfIn: DataFrame, dfZebra: DataFrame): DataFrame = {

    val dfInAgg = agg_date_type_recharge(dfIn, "in")
    val dfZebraAgg = agg_date_type_recharge(dfZebra, "ze")

    dfInAgg.join(dfZebraAgg, Seq("date", "type_recharge"), "full")
  }

  /*

  SELECT date, type_recharge,  from table1 a full join table2 b on a.date = b.date and  a.type_recharge = b.type_recharge

   */


}
