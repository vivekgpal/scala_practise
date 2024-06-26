import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
object spotifyplaylist {
    def main(args : Array[String] ) : Unit = {
      Logger.getLogger("org").setLevel(Level.OFF)
      val spark =  SparkSession
        .builder()
        .master("local[*]")
        .appName("spotify")
        .getOrCreate()
      val df = spark.read.json("D://spotify.json")
      df.show()
  }
}
