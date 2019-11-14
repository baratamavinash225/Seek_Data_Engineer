package com.verizon.ceam.stpRttSubscriberQciEvents

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.verizon.ceam.util.HiveConnection
import com.verizon.ceam.util.Config

object StpRttLoadSubscriberEventsQci extends HiveConnection{

  type OptionMap = Map[Symbol, Any]
  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis()
    val options = readCmdLineArgs(args)
    val configFile=options('configFile).toString
    var date=""
    var hr=""
    var min=""
    if(options.contains('date)){
      date=options('date).toString
      hr=options('hr).toString
      min=options('min).toString
    }
    println("[INFO]: Getting Spark context")
    val spark = createOrGetSparkSession()
    val config = new Config(configFile)

    val pCols = List("p_date_key","subscriber_key","imei","imsi","uectxtrelease_qci_array")
    val pSchema=config.get("STP_RTT_LOAD_QCI_EVENTS_SUBSCRIBER_SOURCE_SCHEMA")
    val pTable=config.get("STP_RTT_LOAD_QCI_EVENTS_SUBSCRIBER_INPUT_HIVE_TABLE")
    val sSchema=config.get("STP_RTT_LOAD_QCI_EVENTS_SUBSCRIBER_SOURCE_SCHEMA")
    val sTable=config.get("STP_RTT_LOAD_QCI_EVENTS_SUBSCRIBER_HIVE_QCI_SNAPSHOT_EXT_OUTTABLE")
    val subSchema=config.get("STP_RTT_LOAD_QCI_EVENTS_SUBSCRIBER_SOURCE_SCHEMA")
    val subTable=config.get("STP_RTT_LOAD_QCI_EVENTS_SUBSCRIBER_SUBSCRIBER_PROFILE_TBL")
    val num_int=config.get("STP_RTT_LOAD_QCI_EVENTS_SUBSCRIBER_MAX_RUN_INTERVALS").toInt
    val ctlFile=config.get("STP_RTT_LOAD_QCI_EVENTS_SUBSCRIBER_HDFS_CTL_FILE_PATH")
    val snapshotOutPath=config.get("STP_RTT_LOAD_QCI_EVENTS_SUBSCRIBER_HDFS_QCI_SNAPSHOT_OUTPATH")
    val historyOutPath=config.get("STP_RTT_LOAD_QCI_EVENTS_SUBSCRIBER_HDFS_QCI_HISTORY_OUTPATH")
    val eventDesc=config.get("STP_RTT_LOAD_QCI_EVENTS_SUBSCRIBER_SPARK_API_DESCRIPTION")
    val eTable=config.get("STP_RTT_LOAD_QCI_EVENTS_SUBSCRIBER_HIVE_QCI_HISTORY_EXT_OUTTABLE")

    var pColStr=""
    var isUpdateCtlFile=0

    if ((date != "") && (hr != "") && (min != "")) {

      println(s"[INFO]: Running with comand-line arguments: Date: $date, Hour: $hr, Min_interval: $min")
      val successRunIntr = date+" "+hr+":"+min
      val partitionIntervals = getNextIntervalsFromCtlFile(spark, "string", successRunIntr, "min", 15, num_int)
      pColStr=partitionIntervals.mkString(",")

    } else {

      println(s"[INFO]: Running from CTL_FILE contents")
      val partitionIntervals = getNextIntervalsFromCtlFile(spark, "file", ctlFile, "min", 15, num_int)
      pColStr=partitionIntervals.mkString(",")
      isUpdateCtlFile=1
    }

    println(s"[INFO]: Intervals: $pColStr")
    val idf = readFromHive(spark,pSchema,pTable,pCols,s"p_date_key in ($pColStr)")

    val df1 = idf.withColumn("mdn",when(col("subscriber_key").startsWith("1"),substring(col("subscriber_key"),2,10))).withColumn("erab", when(col("uectxtrelease_qci_array").contains("9"),"9").otherwise(when(col("uectxtrelease_qci_array").contains("8"),"8").otherwise(lit("NA")))).drop("uectxtrelease_qci_array").drop("subscriber_key").withColumn("p_date_key",col("p_date_key").cast("String"))

    val sdf1 = readFromHive(spark,sSchema,sTable)

    println(s"[INFO]: Going to generate events")
    val (latestSnapshot,latestEvents) = genEvents(spark,df1,sdf1,num_int,eventDesc)

    println(s"[INFO]: Append subscriber ID to events data set")

    /* Append subscriber ID */
    val groupColumns = List("mdn","imei","imsi")
    val ordColumns = List("update_time")
    val finalEvents = getLatestId(spark, subSchema, subTable, latestEvents,"", groupColumns, ordColumns)

    var reorderedColumnNames = Array("event_date_gmt","mdn","subscriber_id","imei","imsi","record_type","event_id","event_type","prev_qci_status","latest_qci_status","event_details","start_date","create_date","is_first_time_load","service_impacted","service_type","resolved_date","resolved_category","severity","event_category","datasource","load_time","event_day")

    val finalDf = finalEvents.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)


    println(s"[INFO]: Load Snapshot and Evnets Datasets to HDFS and Hive")
    /* Write to  Hive */
    latestSnapshot.write.mode("overwrite").insertInto(sSchema+"."+sTable)
    finalDf.write.mode("overwrite").insertInto(sSchema+"."+eTable)

    val ctlDate = getLastSuccessRunDate(spark, latestSnapshot,"start_date")

    if(isUpdateCtlFile==1) {

      println(s"[INFO]: Updating CTL FILE: $ctlFile")
      writeToCtlFile(spark,ctlFile,ctlDate)
    }

    println(s"[INFO]: Successfully loaded Snapshot and Evnets Datasets to HDFS and Hive")

    val endTime = System.currentTimeMillis()
    println(s" Total time taken to complete the process: ${(endTime - startTime) / 1000} seconds.")
  }

  def getLastSuccessRunDate(spark: org.apache.spark.sql.SparkSession, frame: org.apache.spark.sql.DataFrame, tsCol: String) :String = {

    import spark.implicits._
    val ctlDate=frame.withColumn("sdate",from_unixtime(unix_timestamp(col(tsCol),"yyyy-MM-dd HH:mm:ss"),"yyyy-MM-dd HH:mm")).agg(max("sdate") as ("max_start")).map(r => r.getString(0)).first
    ctlDate
  }

  def readCmdLineArgs(args: Array[String]) :OptionMap = {

    val usage = """
    Usage: jar [--config fileName] [-m <date in yyyy-MM-dd> <hour in HH> <min in 00/15/30/45>]
    """
    if (args.length == 0) println(usage)
    val arglist = args.toList

    val options = nextOption(Map(),arglist)
    if(!options.contains('configFile)){
      println("Configuration file is missing. Exiting.")
    } else {
        if((arglist.size > 2) && (!options.contains('date))){
          println(usage)
          System.exit(1)
        } else {
          println(options('configFile))
        }
    }
    options
  }

  def nextOption(map : Map[Symbol, Any], list: List[String]) : OptionMap = {

    def isSwitch(s : String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--config" :: value :: tail =>
                             nextOption(map ++ Map('configFile -> value), tail)
      case "-m" :: value1 :: value2 :: value3 :: tail =>
                             nextOption(map ++ Map('date -> value1) ++ Map('hr -> value2) ++ Map('min -> value3), tail)
      case option :: tail => println("Something is wrong with "+option)
                             map
    }
  }

  def genEvents(spark: org.apache.spark.sql.SparkSession, pSet: org.apache.spark.sql.DataFrame, subSet: org.apache.spark.sql.DataFrame, num_int: Int, eventDesc: String): (org.apache.spark.sql.DataFrame, org.apache.spark.sql.DataFrame)={

      import spark.implicits._
      var df=pSet
      var sdf=subSet
      var finalEventDf=spark.emptyDataFrame
      var finalSnapDf=spark.emptyDataFrame
      for (i <- 1 to num_int){

          /* filter min(p_date_key) set from input data frame */
          val min_val = df.agg(min("p_date_key").as("min_p_date_key"))
          val fil_set = df.orderBy("p_date_key").join(min_val,df.col("p_date_key") === min_val.col("min_p_date_key"),"inner").drop("min_p_date_key").filter(df.col("erab")!=="NA")

          /* Join fil_set with sdf */
          val joined = fil_set.join(sdf,Seq("mdn","imei","imsi"),"full")

          /* Verify whether to generate event or not and generate latest snapshot */
          val newDf = joined
                      .withColumn("create_event", when(((col("latest_qci_status") === "NA") && (col("erab") === col("latest_qci_status"))) || (col("erab") === col("latest_qci_status")), 0).otherwise(when(col("erab").isNull,0).otherwise(1)))
                      .withColumn("is_first_time_load",when(col("latest_qci_status").isNull,1).otherwise(when(col("erab").isNull,col("is_first_time_load")).otherwise(0)))
                      .na.fill("NA",Seq("latest_qci_status","prev_qci_status"))
                      .withColumn("start_date",when(col("erab") !== col("latest_qci_status"),unix_timestamp(col("p_date_key"),"yyyyMMddHHmm").cast("timestamp")).otherwise(col("start_date")))
                      .withColumn("create_date",col("start_date"))
                      .withColumn("load_time",col("start_date"))
                      .withColumn("event_date_gmt",col("start_date"))
                      .withColumn("event_details",when(col("event_details").isNull,concat(lit(eventDesc+" from NA to "), col("erab"))).otherwise(when(col("latest_qci_status") !== col("erab"),concat(lit(eventDesc+" from  "),col("latest_qci_status"),lit(" to "),col("erab"))).otherwise(col("event_details"))))
                      .withColumn("latest_qci_status1",when(((col("erab").isNull)||((col("erab").isNotNull) &&(col("latest_qci_status").isNotNull) &&(col("erab") === col("latest_qci_status")))), col("latest_qci_status")).otherwise(col("erab")))
                      .withColumn("prev_qci_status1", when(((col("erab").isNull) ||(col("erab").isNotNull && col("erab") ===col("latest_qci_status"))),col("prev_qci_status")).otherwise(when(col("latest_qci_status").isNull,lit("NA")).otherwise(col("latest_qci_status"))))
                      .drop("latest_qci_status","prev_qci_status","p_date_key")
                      .withColumnRenamed("latest_qci_status1","latest_qci_status")
                      .withColumnRenamed("prev_qci_status1","prev_qci_status")
                      .withColumn("record_type",lit("QCI SWITCH"))
                      .withColumn("event_type",lit("QCI SWITCH"))
                      .withColumn("event_category", lit("QCI SWITCH"))
                      .withColumn("datasource",lit("RTT"))
                      .withColumn("event_day",from_unixtime(unix_timestamp(col("start_date"),"yyyy-MM-dd HH:mm:ss"),"yyyy-MM-dd"))

          val eDf = newDf.filter((col("is_first_time_load")!==1) && (col("create_event")===1)).drop("is_first_time","create_event")
          var reorderedColumnNames = Array("event_date_gmt","mdn","imei","imsi","record_type","event_id","event_type","prev_qci_status","latest_qci_status","event_details","start_date","create_date","is_first_time_load","service_impacted","service_type","resolved_date","resolved_category","severity","event_category","datasource","load_time","event_day")

          val snapDf = newDf.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)
          val eventDf = eDf.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)

          /* Remove min(p_date_key) set from input data frame */
          val min_list = min_val.select("min_p_date_key").map(r => r.getString(0)).collect.toList
          df = df.filter(!col("p_date_key").isin(min_list:_*))
          sdf = snapDf
          finalSnapDf = sdf
          if (finalEventDf.columns.size == 0){
            finalEventDf=eventDf
          }else{
            finalEventDf=finalEventDf.unionAll(eventDf)
          }
    }
    (finalSnapDf,finalEventDf)
  }
}
