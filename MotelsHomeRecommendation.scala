package com.epam.hubd.spark.scala.sql.homework

import java.text.DecimalFormat

import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.{SparkConf, SparkContext}
import com.epam.hubd.spark.scala.sql.homework.Constants.INPUT_DATE_FORMAT
import com.epam.hubd.spark.scala.sql.homework.Constants.OUTPUT_DATE_FORMAT
import org.apache.spark
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.rank
import org.apache.spark.sql.expressions.Window
import scala.collection.JavaConverters._

object MotelsHomeRecommendation {

  System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
  System.setProperty("HADOOP_HOME", "C:\\hadoop\\bin\\winutils.exe")

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    val sc = new SparkContext(new SparkConf().setAppName("motels-home-recommendation").setMaster("local[*]"))
    val sqlContext = new HiveContext(sc)

    // val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    processData(sqlContext, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }

  def processData(sqlContext: HiveContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: DataFrame = getRawBids(sqlContext, bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      */
    val erroneousRecords: DataFrame = getErroneousRecords(rawBids)
    erroneousRecords.write
      .format(Constants.CSV_FORMAT)
      .save(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: DataFrame = getExchangeRates(sqlContext, exchangeRatesPath)

    /**
      * Task 3:
      * UserDefinedFunction to convert between date formats.
      * Hint: Check the formats defined in Constants class
      */

    // val convertDate:  UserDefinedFunction = getConvertDate[String,String](getConvertDate)
    val convertDate: UserDefinedFunction = udf[String, String](getConvertDate)
    /**
      * Task 3:
      * Transform the rawBids
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: DataFrame = getBids(rawBids, exchangeRates)

    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
    val motels: DataFrame = getMotels(sqlContext, motelsPath)

    /**
      * Task5:
      * Join the bids with motel names.
      */
    val enriched: DataFrame = getEnriched(bids, motels)
    enriched.write
      .format(Constants.CSV_FORMAT)
      .save(s"$outputBasePath/$AGGREGATED_DIR")
  }

  def getRawBids(sqlContext: HiveContext, bidsPath: String): DataFrame = {


    val df = sqlContext.read
      .format("parquet")
      .load(bidsPath)


    df
  }

  def getErroneousRecords(rawBids: DataFrame): DataFrame = {

    val df: DataFrame = rawBids.select("BidDate", "BidDate", "HU").filter("HU like '%ERROR%'").groupBy("BidDate", "HU").count()

    df.coalesce(1)
  }

  def getExchangeRates(sqlContext: HiveContext, exchangeRatesPath: String): DataFrame = {

    val exchangeRateDF = sqlContext.read
      .format("csv")
      .load(exchangeRatesPath)
      .select("_c0", "_c3").toDF("BidDate", "EuroValue")
      .selectExpr("BidDate", "cast(EuroValue as Double)")

    val convertDate: UserDefinedFunction = udf[String, String](getConvertDate)
    val usdToEuroConverted: UserDefinedFunction = udf[Double, Double, Double](usdToEuro)

    sqlContext.udf.register("convertEuro", usdToEuro1(_: Double): Double) //power3(_:Double):Double

    sqlContext.udf.register("convertDate", getConvertDate(_: String))


    exchangeRateDF

  }

  def usdToEuro1(d: Double): Double = {

    val dc = new DecimalFormat("#0.000")
    return dc.format(d).toDouble

  }

  def getBids(rawBids: DataFrame, exchangeRates: DataFrame): DataFrame = {

    val filtered = rawBids.select("MotelID", "BidDate", "HU", "US", "CA", "MX")
      .filter("HU not like '%ERROR%'")

    import org.apache.spark.sql.functions.lit
    val dataUS = filtered.select("MotelID", "BidDate", "US").withColumn("Country", lit("US"))
    val dataCA = filtered.select("MotelID", "BidDate", "CA").withColumn("Country", lit("CA"))
    val dataMX = filtered.select("MotelID", "BidDate", "MX").withColumn("Country", lit("MX"))

    val total = dataUS.union(dataCA).union(dataMX).toDF("MotelID", "BidDate", "Price", "Losa")

    val filteredTotal = total.filter("Price != ''")


    val usdToEuroConverted: UserDefinedFunction = udf[Double, Double, Double](usdToEuro)
    val convertDate: UserDefinedFunction = udf[String, String](getConvertDate)


    val filteredTotalJoinexchangeRates = filteredTotal.join(exchangeRates, "BidDate")
      .selectExpr("MotelID", "BidDate", "Price", "EuroValue", "(Price * EuroValue) as PriceCalculated", "Losa")

    val FinalDF = filteredTotalJoinexchangeRates
      .selectExpr("MotelID", "convertDate(BidDate) as BidDate", "Losa", "convertEuro(PriceCalculated) as PriceCalculatedData")


    FinalDF
  }

  def getConvertDate(bdate: String): String = {

    val jodatime = INPUT_DATE_FORMAT.parseDateTime(bdate)
    val data = OUTPUT_DATE_FORMAT.print(jodatime)

    data
  }

  def usdToEuro(usd: Double, euro: Double): Double = {
    val usdToEuro: Double = usd * euro
    val dc = new DecimalFormat("#0.000")
    return dc.format(usdToEuro).toDouble

  }

  def getMotels(sqlContext: HiveContext, motelsPath: String): DataFrame = {

    val motelsDF = sqlContext.read
      .format("parquet")
      .load(motelsPath)

    motelsDF
  }


  def getEnriched(bids: DataFrame, motels: DataFrame): DataFrame = {


    val joinMotelsBids = bids.join(motels, "MotelID")
      .selectExpr("MotelID", "MotelName", "BidDate", "Losa", "PriceCalculatedData")


    val w = Window.orderBy("PriceCalculatedData")

    val demo = joinMotelsBids.withColumn("rank", rank.over(Window.partitionBy("MotelID", "MotelName", "BidDate").orderBy("PriceCalculatedData"))).where("rank like 1")
      .selectExpr("MotelID", "MotelName", "BidDate", "Losa", "PriceCalculatedData")


    return demo.coalesce(1)
  }
}
