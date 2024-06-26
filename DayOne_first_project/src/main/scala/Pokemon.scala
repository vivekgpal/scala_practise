import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}
object Pokemon {
  def main(args:Array[String])={
    Logger.getLogger("org").setLevel(Level.OFF)
      val spark = establishSession;

      val df = spark
        .read
        .option("header",true)
        .csv("C:/excel_files/pokemon.csv")
    val head = df.first()
    println(head)
   val df1 = df.filter(d => !d.equals(head))

    df.createOrReplaceTempView("pokemon")
 df1.printSchema()

//    val count_of_water = count(spark,"Water").first().getLong(0)
//    val count_of_fire = count(spark,"Fire").first().getLong(0)
//    val population_of_fire = populationCount(spark,"Fire").first().getLong(0)
//val max_defence = spark.sql(
//  """
//    |select count(*) from pokemon
//    |""".stripMargin).show()

//
//   print(df)
//    print(df.rdd)
val rdd = df.rdd




//    val max_defence = spark.sql(
//  """
//    |select Name,max(cast(defense as int))  as defense from pokemon  group by name order by defense desc limit 10
//    |""".stripMargin).show()

  }
  def establishSession :SparkSession  ={
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("pokemon")
      .getOrCreate()
    return spark
  }
  def count(spark:SparkSession,filter:String): DataFrame = {
    val sql =
      s"""
        |select
        |  count(*)
        |from
        | pokemon
        |where `type 1`="$filter"
        |""".stripMargin
    return spark.sql(sql)
  }
  def populationCount(spark:SparkSession,filter:String):DataFrame={
    val sql =
      s"""
        |select
        | cast(sum(Total) as int)
        |from
        | pokemon
        |where `Type 1` = "$filter"
        |""".stripMargin
    return spark.sql(sql)
  }
}
