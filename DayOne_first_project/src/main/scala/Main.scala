import org.apache.log4j.{Level,Logger,LogManager}
import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.spark.sql.SaveMode
object Main {
  def main(args:Array[String]): Unit = {
   Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.OFF)
    LogManager.getLogger("etl tools")  //builder=>Configure SparkSession properties using builder methods
    val spark: SparkSession = SparkSession.builder()  //ss is entrypoint
      .master("local[*]")  //Sets the master URL to run Spark locally with all available cores
      .appName("csv")
      .getOrCreate();   //After configuring the properties using the builder methods,
                      // you call getOrCreate() to actually create the SparkSession instance
                       //based on the specified configurations.
                      //If a SparkSession already exists (e.g., in the same JVM),
                      // getOrCreate() returns the existing one; otherwise, it creates a new one.


    val df = spark      //to create dataframe
            .read
            .option("header",true)
            .csv("C:/Users/8002263/Downloads/Fw_ Practice Dataset/addresses.csv")

    //df.printSchema()
    //df.show(10)

    df.createOrReplaceTempView("addresses")

    val res = spark.sql(
      """
        |select name,lastname from addresses where name is not null
        |""".stripMargin).collect().map(x=>x(0).toString).toList

  println(res)



//  to convert dataframe to rdd
  /*  val rdd = df.rdd

      df.createOrReplaceTempView("addresses")
      df.printSchema()
    df.select("name").where("name=='john'").show(10)

    val dataTable = spark.sql(""" SELECT * FROM addresses """).show()


   val totalCountOfRecords =  spark.sql(
      """SELECT count(*) FROM addresses """   //where trim(name)='John'
    ).first().getLong(0)

  println("Total numbers of record : "+totalCountOfRecords)

    val countOfDuplicateByName = spark.sql(
      """SELECT COUNT(*) FROM ( SELECT name,COUNT(name) as count_of_name FROM addresses GROUP BY name HAVING count_of_name >1) """
    ).first().getLong(0)
    println(s"count of duplicate by name : $countOfDuplicateByName")

    val countOfDuplicateByLastName = spark.sql(
      """SELECT COUNT(*) FROM ( SELECT lastname,COUNT(lastname) as count_of_name FROM addresses GROUP BY lastname HAVING count_of_name >1) """
    ).first().getLong(0)
    println(s"count of duplicate by lastname : $countOfDuplicateByLastName")

    val countOfDuplicateByAddresses = spark.sql(
      """SELECT COUNT(*) FROM ( SELECT address,COUNT(address) as count_of_name FROM addresses GROUP BY address HAVING count_of_name >1) """
    ).first().getLong(0)
    println(s"count of duplicate by address : $countOfDuplicateByAddresses")

    val countOfDuplicateByCity = spark.sql(
      """SELECT COUNT(*) FROM ( SELECT city,COUNT(city) as count_of_name FROM addresses GROUP BY city HAVING count_of_name >1) """
    ).first().getLong(0)
    println(s"count of duplicate by city : $countOfDuplicateByCity")

      val countOfDuplicateByLocationCode = spark.sql(
      """SELECT COUNT(*) FROM ( SELECT TRIM(locationcode) as locationcode,COUNT(TRIM(locationcode)) as count_of_name FROM addresses GROUP BY TRIM(locationcode) HAVING count_of_name >1) """
    ).first().getLong(0)
    println(s"count of duplicate by locationcode : $countOfDuplicateByLocationCode")
    val countOfDuplicateByPinCode = spark.sql(
      """SELECT COUNT(*) FROM ( SELECT pincode,COUNT(pincode) as count_of_name FROM addresses GROUP BY pincode HAVING count_of_name >1) """
    ).first().getLong(0)
    println(s"count of duplicate by pincode : $countOfDuplicateByPinCode")
    val countOfDuplicateByDOB = spark.sql(
      """SELECT COUNT(*) FROM ( SELECT DOB,COUNT(DOB) as count_of_name FROM addresses GROUP BY DOB HAVING count_of_name >1) """
    ).first().getLong(0)
    println(s"count of duplicate by DOB : $countOfDuplicateByDOB")

    import spark.implicits._
    val result = Seq(("Total numbers of record",totalCountOfRecords),
                      ("count of duplicate by name",countOfDuplicateByName) ,
                      ("count of duplicate by address",countOfDuplicateByAddresses),
                      ("count of duplicate by city",countOfDuplicateByCity),
                      ("count of duplicate by locationcode",countOfDuplicateByLocationCode),
                      ("count of duplicate by pincode",countOfDuplicateByPinCode),
                      ("count of duplicate by DOB",countOfDuplicateByDOB)

                    ).toDF("Description","count")
*/
   /* result
      .coalesce(1)      //if not write then all seq will store in different different excel sheet
      .write
      .option("header",true)
      .mode(SaveMode.Ignore)    //if already exist then ignore
      .csv("C:/Users/8002263/Downloads/Fw_ Practice Dataset/result.csv")

    result.coalesce(1)
      .write
      .option("header",true)
      .mode(SaveMode.Ignore)
      .parquet("C:/Users/8002263/Downloads/Fw_ Practice Dataset/result") */




 }
}