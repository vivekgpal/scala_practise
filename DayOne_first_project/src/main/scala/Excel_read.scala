import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, collect_list}
import org.apache.spark.sql.functions.{col, lit, substring, when}
object Excel_read {
  def main(args:Array[String]):Unit ={
    Logger.getLogger("org").setLevel(Level.OFF)
   val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .appName("Read_Excel_file")
                  .getOrCreate()

   var df = spark
     .read
     .format("com.crealytics.spark.excel")
     .option("header",true)
     .load("C:/Users/8002263/Downloads/Fw_ Practice Dataset/STORE.xlsx")

    //df = df.filter(col("Order Priority")==="High")
    df.show()

//  df.printSchema()
   df.createOrReplaceTempView("store")
//    spark.sql("""select * from store """).show(10)
//    val totalCount = spark.sql(""" select count(*) from store """).first().getLong(0)
//    println(s"total count of records : $totalCount")

//  var countOfColumn = spark.sql("select * from store")

//    df.columns.foreach(col =>{
//      var query = s"""select `$col`,count(*)  from store group by `$col`"""
//
//      var count = spark.sql(query).first().getLong(0)
//      println(s"count of duplicate by $col : $count")
//
//
//    })
//    df.printSchema()
//    val upd =df.withColumn("Quantity ordered new",col("Quantity ordered new").cast("int"))
//    upd.groupBy("City","Quantity ordered new").count().groupBy("City").agg(collect_list("Quantity ordered new").alias("Quantity ordered new")).show()




  }


}
