package com.epam.hubd.spark.scala.sql.homework

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object MotelsHomeRecommendation {

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    val sc = new SparkContext(new SparkConf().setAppName("motels-home-recommendation"))
    val sqlContext = new HiveContext(sc)

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
    val convertDate: UserDefinedFunction = getConvertDate

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

  //Task 1_1 impl
  def getRawBids(sqlContext: HiveContext, bidsPath: String): DataFrame = {

    //read input parquet file using DataSource API
    sqlContext.read.parquet(bidsPath)
  }

  //Task 1_2 impl
  def getErroneousRecords(rawBids: DataFrame): DataFrame = {
    import rawBids.sqlContext.implicits._

    //filter - get only rows with "ERROR"
    //select - leave necessary columns
    //groupBY - to count errors by date and error message
    //count - count them
    //coalesce - output to one file (in future write)
    rawBids
      .filter(row => row.toString().contains("ERROR"))
      .select(rawBids("BidDate"), $"HU".alias("Error"))
      .groupBy("BidDate", "Error")
      .count()
      .coalesce(1)
  }

  //Task 2 impl
  def getExchangeRates(sqlContext: HiveContext, exchangeRatesPath: String): DataFrame = {
    import sqlContext.implicits._

    //read file using Constants.CSV_FORMAT, load data
    //leave necessary columns for future join - date and rate
    sqlContext.read
      .format(Constants.CSV_FORMAT)
      .load(exchangeRatesPath)
      .select($"_c0".as("date"), $"_c3".as("rate"))
  }

  //Task 3_1 impl
  def getConvertDate: UserDefinedFunction = {

    //create custom UDF that converts Date
    udf((date: String) => Constants.INPUT_DATE_FORMAT.parseDateTime(date).toString(Constants.OUTPUT_DATE_FORMAT))
  }


  //Task 3_2 impl
  def getBids(rawBids: DataFrame, exchangeRates: DataFrame): DataFrame = {
    import rawBids.sqlContext.implicits._

    //get custom UDF to use it in next steps
    val convertDate = MotelsHomeRecommendation.getConvertDate

    //filter - get rid of rows with errors
    //join - join with exchangeRates DataFrame to get rate column
    //explode - add additional columns with LoSa name (column _1) and their values (column _2)
    //withColumn - add column Price with converted price
    //select - leave necessary columns
    //where - get rids of wors with null price
    rawBids
      .filter(row => !row.toString().contains("ERROR"))
      .join(exchangeRates, $"BidDate" === $"date")
      .explode($"US", $"MX", $"CA")(row => Array(("US", row.getString(0)), ("MX", row.getString(1)), ("CA", row.getString(2))))
      .withColumn("Price", round($"_2" * $"rate", 3))
      .select($"MotelID", convertDate($"BidDate").as("BidDate"), $"_1".as("LoSa"), $"Price")
      .where($"Price".isNotNull)
  }

  //Task 4 impl
  def getMotels(sqlContext: HiveContext, motelsPath: String): DataFrame = {

    //read parquet file
    //leave necessary columns for future join
    sqlContext.read.parquet(motelsPath)
      .select("MotelID", "MotelName")
  }

  //Task 5 impl
  def getEnriched(bids: DataFrame, motels: DataFrame): DataFrame = {
    import bids.sqlContext.implicits._

    //create window for next steps
    val windowSpec = Window.partitionBy(bids("MotelID"), bids("BidDate")).orderBy(bids("Price").desc)

    //join - join with motels DataFrame on MotelID to get MotelName
    //select - leave necessary columns in proper order, and set rank to each price using window (rows grouped by MotelID and BidDate)
    //where - leave only rows with rank=1 (max price in group)
    //drop - get rid of columns rank as we do not need it anymore
    //coalesce - output to one file (in future write)
    bids.join(motels, "MotelID")
      .select($"MotelID", $"MotelName", $"BidDate", $"LoSa", $"Price", rank.over(windowSpec).as("rank"))
      .where($"rank".equalTo(1))
      .drop("rank")
      .coalesce(1)
  }
}
