
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, trim, upper, when}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
object Brand {
  def main(args:Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Brand")
      .getOrCreate()

  val string = "20.4";
    val float = string.toFloat

    val integer = float.toInt
    print(s"num : $integer, newnum : $string,nn : $float ")
   /* val df_schema = StructType(
      Seq(
        StructField("Brand1",StringType,nullable = true),
        StructField("Brand2",StringType,nullable = true),
        StructField("Year",IntegerType,nullable=true),
        StructField("custom1",IntegerType,nullable = true),
        StructField("custom2",IntegerType,nullable = true),
        StructField("custom3",IntegerType,nullable = true),
        StructField("custom4",IntegerType,nullable = true),
      )
    )*/
    /*var df = spark
      .read
      .format("com.crealytics.spark.excel")
      .schema(df_schema)
      .option("header",true)
      .load("C:/excel_files/day1_brand.xlsx")

    df = df.withColumn("Brand1",upper(trim(col("Brand1"))))
      .withColumn("Brand2",upper(trim(col("Brand2"))))
    df.show()*/
    //df.printSchema()
//    df = df.withColumn("Year",when(trim(col("Brand1"))==="apple",null).otherwise(col("Year")))
//    df.show()
//    df = df.filter(col("Brand2").isNotNull && col("Year").isNotNull)
//    df.show()
  // df.select("Brand1").show()
//    df.createOrReplaceTempView("brand")
//    spark.sql(
//      """ select * from brand """
//    ).show()


     // df.select("Brand1","Brand2").where(col("custom1")===1).show()
   // df.groupBy(col("Brand1")).agg(sum("custom1").alias("custom1")).show()
    //df.withColumn("newcol",sum("custom1").over(Window.partitionBy("custom1")).alias("newcol")).show()
  }

}
