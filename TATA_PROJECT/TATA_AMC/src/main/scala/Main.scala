package Master

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.types.StringType

object Main {

  def main(args: Array[String]): Unit = {

    // Set log level to avoid excessive logging
    Logger.getLogger("org").setLevel(Level.OFF)

    // Create a Spark session
    val spark = SparkSession
      .builder()
      .master("local[*]") // Set the appropriate master URL
      .appName("Read_Excel_file")
      .getOrCreate()

    // Read data from a CSV file
    val df = spark
      .read
      .option("header", true)
      .csv("C:/Users/8002263/IdeaProjects/TATA_PROJECT/transaction_type_master_new.csv")

    // Create a temporary view for querying using SQL
    df.createOrReplaceTempView("transaction_type_master_new")

    // Transform column names and create a new DataFrame
    var STG_S_TRXN_TYPE:DataFrame = spark.sql(
      """
        |SELECT
        |  transaction_code AS STT_TRXN_TYPE_CODE,
        |  trxn_subtype_code AS STT_TRXN_SUBTYPE_CODE,
        |  trxn_status AS STT_TRXN_STATUS,
        |  trxn_type_flag AS STT_TRXN_TYPE_FLAG,
        |  collection_bank_code AS STT_COLLECTION_BANK_CODE,
        |  trxn_table AS STT_TRXN_TABLE,
        |  transaction_mode AS STT_TRXN_MODE,
        |  transaction_type AS STT_TRXN_TYPE,
        |  transaction_group AS STT_TRXN_GROUP,
        |  sales AS STT_NET_SALES,
        |  aum AS STT_AUM
        |FROM transaction_type_master_new
        |""".stripMargin)

    val count = STG_S_TRXN_TYPE.count()
  println(count)
    STG_S_TRXN_TYPE.createOrReplaceTempView("STG_S_TRXN_TYPE")
    // Delete rows where STT_TRXN_TYPE_CODE is 'NA'
    STG_S_TRXN_TYPE.show(50)
//    STG_S_TRXN_TYPE = STG_S_TRXN_TYPE.na.drop()

   //STG_S_TRXN_TYPE.filter(col("STT_COLLECTION_BANK_CODE")="Null")
  println(dtype(STG_S_TRXN_TYPE))

  }
}
