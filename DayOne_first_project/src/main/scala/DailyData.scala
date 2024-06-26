import com.sun.org.apache.xalan.internal.lib.ExsltDatetime.{date, dateTime}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.catalyst.expressions.Month
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Calendar
import scala.+:
object DailyData {
  def main(args:Array[String]):Unit ={
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder()
      .appName("CreateTableExample")
      .master("local[*]") // Set your master URL
      .getOrCreate()
 //   val col = Seq("year","month","date","day")





//    df.show()
//   val last_day_of_month = List(
//     1 -> 31,
//     2 -> 28,
//     3 -> 31,
//     4 -> 30,
//     5 -> 31,
//     6 ->30,
//     7->31,
//     8->31,
//     9->30,
//     10->31,
//     11->30,
//     12->31,
//   )
//    val week = Map(
//      1->"mon",
//      2->"tue",
//      3->"wed",
//      4->"thru",
//      5->"fri",
//      6->"sat",
//      0->"sun"
//    )
//    //var data: Seq[(Int, Int, Int, String)] = Seq((2000, 1, 1, "mon"))
//    var data: Seq[(Int, Int, Int, String)] = Seq.empty
//    var year = 2022
//    var j=0;
//    while(year<=2024){
//      last_day_of_month.foreach{
//        case (key,value)=> for(i <- 1 to value){
//          if(j>=8) j=0
//          var currdate = s"$i-$key-$year"
//         data = data :+ (year,key,i,week(i%7))
////          println(data)
//          j=j+1
//        }
//      }
//      year = year +1
//    }
//
//    val df = spark.createDataFrame(data).toDF(col: _*)
//    df.show()


 // val curr = LocalDateTime.now()
//    val getDayOfWeek = cal.getDayOfWeek  //friday
//    val dayOfMonth = cal.getDayOfMonth  // 03
//    val dayOfYear = cal.getDayOfYear
//println(cal.plusDays(1))
//
//    println(dayOfYear)
//    val startDate = ("2022-05-04T21:14:40.298")
//    val endDate = date("2024-05-04T21:14:40.298")
    // Given date string
    val dateStringstart = "2022-05-04T21:14:40.298"


    // Define the formatter for the input date format
    val inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")

    // Parse the date string into a LocalDateTime
    var startDate = LocalDateTime.parse(dateStringstart, inputFormatter)
    //val endDate = LocalDateTime.parse(dateStringEnd,inputFormatter)
    val endDate = LocalDateTime.now()


   // var data: Seq[(Int, Month, LocalDate, DayOfWeek)] = Seq.empty
    var data: Seq[(Int, Int, Int, String)] = Seq.empty
    while(startDate.toLocalDate!=endDate.toLocalDate){
        data = data :+ (startDate.getYear,startDate.getMonthValue,startDate.getDayOfMonth,startDate.getDayOfWeek.toString)
        startDate = startDate.plusDays(1)

    }
    val col = Seq("year","month","date","day")
    val df = spark.createDataFrame(data).toDF(col: _*)
    df.show()

  }
}

//    val cal = Calendar.getInstance()
//    val date = cal.get(Calendar.DATE)
//    val Year = cal.get(Calendar.YEAR)
//    val Month1 = cal.get(Calendar.MONTH)
//
//    val Month = Month1 + 1
//  println(Calendar.getInstance())
//  println(s"$date,$Year,$Month,$Month1")
//  println(date+" "+Year +" "+ Month)
//
//    df.show()

//    val values = List("slno", "location", "nimf_location", "nimf_locationid", "t15b15", "cams_assetclass", "nimf_assetclass1", "nimf_assetclass2", "sebi_category", "oe_ce_int", "brokercode", "brokername", "brokersegment", "crmaccountid", "nimf_rmid", "nimf_rmname", "clgassets_this as clg_assets_this", "clgassets_all as clg_assets_all", "sales_this", "sales_all", "red_this", "red_all", "trxns_this", "trxns_all", "folios_this", "folios_all", "net_switch_this as netswitch_this", "net_switch_all as netswitch_all", "divreinv_this", "divreinv_all", "avgage_this", "avgage_all", "avgassets_this", "avgassets_all", "switch_in_this as switchin_this", "switch_in_all as switchin_all", "switch_out_this as switchout_this", "switch_out_all as switchout_all", "new_sip_this as newsip_this", "new_sip_all as newsip_all", "new_sip_amount_this as newsip_amount_t", "new_sip_amount_all as newsip_amount_a", "total_sip_this as totalsip_this", "total_sip_all as totalsip_all", "sip_average_this as sipavg_this", "sip_average_all as sipavg_all", "sip_value_this as sipvalue_this", "sip_value_all as sipvalue_all", "sip_failed_this as sipfailed_this", "sip_failed_all as sipfailed_all", "sip_ceased_this as sipceased_this", "sip_ceased_all as sipceased_all", "systematic_aum_this as sipaum_this", "systematic_aum_all as sipaum_all", "sales_count_this as p_count_t", "sales_count_all as p_count_a", "fresh_purch_count_this as fp_count_t", "fresh_purch_count_all as fp_count_a", "switch_in_count_this as siti_count_t", "switch_in_count_all as siti_count_a", "sip_trig_count_this as siptrig_count_t", "sip_trig_count_all as siptrig_count_a", "sip_trig_amt_this as siptrig_amt_t", "sip_trig_amt_all as siptrig_amt_a", "stp_trig_count_this as stptrig_count_t", "stp_trig_count_all as stptrig_count_a", "stp_trig_amt_this as stptrig_amt_t", "stp_trig_amt_all as stptrig_amt_a", "new_stp_count_this as new_stp_count_t", "new_stp_count_all as new_stp_count_a", "total_stp_count_this as total_stp_count_t", "total_stp_count_all as total_stp_count_a", "stp_ceased_count_this as ceased_stp_count_t", "stp_ceased_count_all as ceased_stp_count_a", "sip_average_size_this as sip_average_size_t", "sip_average_size_all as sip_average_size_a", "densrank", "agentrefid", "business_segment", "wealth_retail", "inhousecode","run_week" ,"run_month", "run_year")
//
////    for(x <- values){
////      println(x)
////    }
//      values.foreach((elt)=>println(elt))
//    //println(values(2))
//
//    val monthMap = Map("january" -> "01",
//      "02" -> "february",
//      "03" -> "march",
//      "04" -> 6,
//      "05" -> "may"
//    )
//    for(x<-monthMap){
//      println(x)
//
//    }
//    monthMap.foreach{
//      case (key, value) => println (key + " -> " + value)
//    }
//   // print(monthMap("02"))
//
//
//    val columnsCastDecimal = Seq("clg_assets_this","clg_assets_all","sales_this","sales_all","red_this","red_all","netswitch_this","netswitch_all","divreinv_this","divreinv_all","avgage_this","avgage_all","avgassets_this","avgassets_all","switchin_this","switchin_all","switchout_this","switchout_all","newsip_amount_t","newsip_amount_a","sipavg_this","sipavg_all","sipvalue_this","sipvalue_all","sipaum_this","sipaum_all","siptrig_amt_t","siptrig_amt_a","stptrig_amt_t","stptrig_amt_a","sip_average_size_t","sip_average_size_a")
//
//    def cams_monthly_columns_to_select(db: String, table: String): List[String] = {
//      val values = List("slno", "period", "location", "nimf_location", "nimf_locationid", "t15b15", "cams_assetclass", "nimf_assetclass1", "nimf_assetclass2", "sebi_category", "oe_ce_int", "brokercode", "brokername", "brokersegment", "crmaccountid", "nimf_rmid", "nimf_rmname", "clgassets_this as clg_assets_this", "clgassets_all as clg_assets_all", "sales_this", "sales_all", "red_this", "red_all", "trxns_this", "trxns_all", "folios_this", "folios_all", "net_switch_this as netswitch_this", "net_switch_all as netswitch_all", "divreinv_this", "divreinv_all", "avgage_this", "avgage_all", "avgassets_this", "avgassets_all", "switch_in_this as switchin_this", "switch_in_all as switchin_all", "switch_out_this as switchout_this", "switch_out_all as switchout_all", "new_sip_this as newsip_this", "new_sip_all as newsip_all", "new_sip_amount_this as newsip_amount_t", "new_sip_amount_all as newsip_amount_a", "total_sip_this as totalsip_this", "total_sip_all as totalsip_all", "sip_average_this as sipavg_this", "sip_average_all as sipavg_all", "sip_value_this as sipvalue_this", "sip_value_all as sipvalue_all", "sip_failed_this as sipfailed_this", "sip_failed_all as sipfailed_all", "sip_ceased_this as sipceased_this", "sip_ceased_all as sipceased_all", "systematic_aum_this as sipaum_this", "systematic_aum_all as sipaum_all", "sales_count_this as p_count_t", "sales_count_all as p_count_a", "fresh_purch_count_this as fp_count_t", "fresh_purch_count_all as fp_count_a", "switch_in_count_this as siti_count_t", "switch_in_count_all as siti_count_a", "sip_trig_count_this as siptrig_count_t", "sip_trig_count_all as siptrig_count_a", "sip_trig_amt_this as siptrig_amt_t", "sip_trig_amt_all as siptrig_amt_a", "stp_trig_count_this as stptrig_count_t", "stp_trig_count_all as stptrig_count_a", "stp_trig_amt_this as stptrig_amt_t", "stp_trig_amt_all as stptrig_amt_a", "new_stp_count_this as new_stp_count_t", "new_stp_count_all as new_stp_count_a", "total_stp_count_this as total_stp_count_t", "total_stp_count_all as total_stp_count_a", "stp_ceased_count_this as ceased_stp_count_t", "stp_ceased_count_all as ceased_stp_count_a", "sip_average_size_this as sip_average_size_t", "sip_average_size_all as sip_average_size_a", "densrank", "agentrefid", "business_segment", "wealth_retail", "inhousecode", "run_month", "run_year")
//      values
//    }
