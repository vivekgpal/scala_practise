import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StringType, StructType}


object cacheExample {
  def main(args:Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

      val spark = SparkSession.builder()
        .master("local[*]")
        .appName("cacheExample")
        .getOrCreate()


    val data = Seq(Row("James", "", "Smith", 2018, 1, "M", 3000),
      Row("Michael ", "Rose", "", 2010, 3, "M", 4000),
      Row("Robert ", "", "Williams", 2010, 3, "M", 4000),
      Row("Maria ", "Anne", "Jones", 2005, 5, "F", 4000),
      Row("Jen", "Mary", "Brown", 2010, 7, "", -1)
    )

    val dataRows = Seq(Row(Row("James;", "", "Smith"), "36636", "M", "3000"),
      Row(Row("Michael", "Rose", ""), "40288", "M", "4000"),
      Row(Row("Robert", "", "Williams"), "42114", "M", "4000"),
      Row(Row("Maria", "Anne", "Jones"), "39192", "F", "4000"),
      Row(Row("Jen", "Mary", "Brown"), "", "F", "-1")
    )

    val columns = Seq("firstname", "middlename", "lastname", "dob_year", "dob_month", "gender", "salary")
    import spark.sqlContext.implicits._
//    val df = data.toDF(columns: _*)
    println(data)
    println(dataRows)

    val schema = new StructType()
      .add("name", new StructType()
        .add("firstname", StringType)
        .add("middlename", StringType)
        .add("lastname", StringType))
      .add("dob", StringType)
      .add("gender", StringType)
      .add("salary", StringType)

  }

}
