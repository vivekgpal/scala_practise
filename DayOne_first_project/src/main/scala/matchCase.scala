object matchCase {

  def main(args :Array[String]): Unit = {
 /*   val t = "hello"
    t match {
      case "firs" => println("hello")
      case "hello" => println(t)
    }
    val num: Option[Int] = Some(11)*/
   // println(num.getOrElse(5))
   val values = List("trs_scheme", "trs_plan", "trs_acno", "coalesce(trs_pbranch,trs_branch) as trs_pbranch", "coalesce(trs_pprxybranch,trs_prxybranch) as trs_pprxybranch", "coalesce(trs_pagent,trs_agent) as trs_pagent", "coalesce(trs_priacode,trs_riacode) as trs_priacode", "trs_purred", "trs_units as units" ,"trs_is_sip")
    println(values.mkString(","))
    //df.withColumn("pan", regexp_replace(col("pan"), "^\\s+|\\s+$", "")
  }

}
