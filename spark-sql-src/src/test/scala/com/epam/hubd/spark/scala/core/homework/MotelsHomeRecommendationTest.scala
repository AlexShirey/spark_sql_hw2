package com.epam.hubd.spark.scala.sql.homework

import java.io.File

import com.epam.hubd.spark.scala.core.util.RddComparator
import com.epam.hubd.spark.scala.sql.homework.MotelsHomeRecommendation.{AGGREGATED_DIR, ERRONEOUS_DIR}
import com.epam.hubd.spark.scala.sql.homework.MotelsHomeRecommendationTest._
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, RDDComparisons}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit._
import org.junit.rules.TemporaryFolder
import org.scalatest.FunSuite


/**
  * Created by Csaba_Bejan on 8/22/2016.
  */
class MotelsHomeRecommendationTest extends FunSuite with DataFrameSuiteBase {
  val _temporaryFolder = new TemporaryFolder

  @Rule
  def temporaryFolder = _temporaryFolder

  val INPUT_BIDS_SAMPLE = "src/test/resources/bids_sample.txt"
  val INPUT_BIDS_SAMPLE_PARQUET = "src/test/resources/bids_sample.parquet"
  val INPUT_RATES_SAMPLE = "src/test/resources/rates_sample.txt"
  val INPUT_MOTELS_SAMPLE = "src/test/resources/motels_sample.txt"
  val INPUT_MOTELS_SAMPLE_PARQUET = "src/test/resources/motels_sample.parquet"

  val INPUT_BIDS_INTEGRATION = "src/test/resources/integration/input/bids.gz.parquet"
  val INPUT_EXCHANGE_RATES_INTEGRATION = "src/test/resources/integration/input/exchange_rate.txt"
  val INPUT_MOTELS_INTEGRATION = "src/test/resources/integration/input/motels.gz.parquet"

  val EXPECTED_AGGREGATED_INTEGRATION = "src/test/resources/integration/expected_output/aggregated"
  val EXPECTED_ERRORS_INTEGRATION = "src/test/resources/integration/expected_output/expected_sql"

  private var outputFolder: File = null

  @Before
  def setup() = {
    outputFolder = temporaryFolder.newFolder("output")
  }

  //test for task 1_1
  @Test
  def shouldReadRawBids() = {
    import java.nio.file.{Paths, Files}

    //we have only bid_sample.txt file, but we should test method that creates DataFrame from Parquet file,
    //so we have to create parquet file as input for getRawBids method
    if (!Files.exists(Paths.get(INPUT_BIDS_SAMPLE_PARQUET))) {
      Helper.createBidsParquetFromText(INPUT_BIDS_SAMPLE, INPUT_BIDS_SAMPLE_PARQUET)
    }

    val expected = sqlTestContext.createDataFrame(Helper.rawBidsData, Helper.rawBidSchema)
    val rawBids = MotelsHomeRecommendation.getRawBids(sqlTestContext, INPUT_BIDS_SAMPLE_PARQUET)

    assertDataFrameEquals(expected, rawBids)
  }

  //test for task 1_2
  @Test
  def shouldCollectAndCountErrors() = {

    val rawBidsData = scTest.parallelize(
      Seq(
        Row("1", "06-05-02-2016", "ERROR_1"),
        Row("2", "15-04-08-2016", "0.89"),
        Row("3", "07-05-02-2016", "ERROR_2"),
        Row("4", "06-05-02-2016", "ERROR_1"),
        Row("5", "06-05-02-2016", "ERROR_2")
      )
    )

    val rawBidsSchema = StructType(
      Array(StructField("MotelID", StringType, nullable = true),
        StructField("BidDate", StringType, nullable = true),
        StructField("HU", StringType, nullable = true))
    )

    val expectedData = scTest.parallelize(
      Seq(
        Row("06-05-02-2016", "ERROR_2", 1L),
        Row("07-05-02-2016", "ERROR_2", 1L),
        Row("06-05-02-2016", "ERROR_1", 2L))
    )

    val expectedSchema = StructType(
      Array(StructField("BidDate", StringType, nullable = true),
        StructField("Error", StringType, nullable = true),
        StructField("count", LongType, nullable = false))
    )

    //input
    val rawBids = sqlTestContext.createDataFrame(rawBidsData, rawBidsSchema)

    val expected = sqlTestContext.createDataFrame(expectedData, expectedSchema)
    val errors = MotelsHomeRecommendation.getErroneousRecords(rawBids)

    assertDataFrameEquals(expected, errors)
  }

  //test for task 2
  @Test
  def shouldReadRatesAndReturnDateAndRate() = {

    val expected = sqlTestContext.createDataFrame(Helper.ratesData, Helper.ratesSchema)
    val exchangeRates = MotelsHomeRecommendation.getExchangeRates(sqlTestContext, INPUT_RATES_SAMPLE)

    assertDataFrameEquals(expected, exchangeRates)
  }

  //test for task 3_1
  @Test
  def shouldDefineConvertDateUDF() = {

    val dateData = scTest.parallelize(
      Seq(
        Row("11-06-05-2016"))
    )

    val dateSchema = StructType(
      Array(StructField("date", StringType, nullable = true))
    )

    val data = sqlTestContext.createDataFrame(dateData, dateSchema)
    val convertDate = MotelsHomeRecommendation.getConvertDate

    val expected = "[2016-05-06 11:00]"
    val actual = data.select(convertDate(data("date"))).collectAsList().get(0).toString()

    assert(expected, actual)
  }

  //test for task 3_2
  @Test
  def shouldGetExpoldedBidsAndConvertDateAndPrice() = {

    val expectedData = scTest.parallelize(
      Seq(
        Row("0000004", "2016-08-04 21:00", "US", 1.397),
        Row("0000004", "2016-08-04 21:00", "MX", 1.563),
        Row("0000004", "2016-08-04 21:00", "CA", 0.611))
    )

    val expectedSchema = StructType(
      Array(StructField("MotelID", StringType, nullable = true),
        StructField("BidDate", StringType, nullable = true),
        StructField("LoSa", StringType, nullable = true),
        StructField("Price", DoubleType, nullable = true))
    )

    //input
    val rawBids = sqlTestContext.createDataFrame(Helper.rawBidsData, Helper.rawBidSchema)
    val rates = sqlTestContext.createDataFrame(Helper.ratesData, Helper.ratesSchema)

    val expected = sqlTestContext.createDataFrame(expectedData, expectedSchema)
    val bids = MotelsHomeRecommendation.getBids(rawBids, rates)

    assertDataFrameEquals(expected, bids)
  }


  //test for task 4
  @Test
  def shouldReadMotels() = {
    import java.nio.file.{Paths, Files}

    //we have only motels_sample.txt file, but we should test method that creates DataFrame from Parquet file,
    //so we have to create parquet file as input for getMotels method
    if (!Files.exists(Paths.get(INPUT_MOTELS_SAMPLE_PARQUET))) {
      Helper.createMotelsParquetFromText(INPUT_MOTELS_SAMPLE, INPUT_MOTELS_SAMPLE_PARQUET)
    }

    val expectedData = scTest.parallelize(
      Seq(
        Row("0000001", "Olinda Windsor Inn"),
        Row("0000002", "Merlin Por Motel"),
        Row("0000003", "Olinda Big River Casino"))
    )

    val expectedSchema = StructType(
      Array(StructField("MotelID", StringType, nullable = true),
        StructField("MotelName", StringType, nullable = true))
    )

    val expected = sqlTestContext.createDataFrame(expectedData, expectedSchema)
    val motels = MotelsHomeRecommendation.getMotels(sqlTestContext, INPUT_MOTELS_SAMPLE_PARQUET)

    assertDataFrameEquals(expected, motels)
  }

  //test for task 5
  @Test
  def shouldGetEnrichedBidsWithMaxPrice() = {

    val bidsData = scTest.parallelize(
      Seq(
        Row("0000001", "2016-06-02 11:00", "MX", 1.50),
        Row("0000001", "2016-06-02 11:00", "US", 1.50),
        Row("0000001", "2016-06-02 11:00", "CA", 1.15),
        Row("0000005", "2016-06-02 12:00", "MX", 1.10),
        Row("0000005", "2016-06-02 12:00", "US", 1.20),
        Row("0000005", "2016-06-02 12:00", "CA", 1.30)
      )
    )

    val motelsData = scTest.parallelize(
      Seq(
        Row("0000001", "Fantastic Hostel"),
        Row("0000005", "Majestic Ibiza Por Hostel"),
        Row("00000010", "Tree Hotel")
      )
    )

    val expectedData = scTest.parallelize(
      Seq(
        Row("0000005", "Majestic Ibiza Por Hostel", "2016-06-02 12:00", "CA", 1.30),
        Row("0000001", "Fantastic Hostel", "2016-06-02 11:00", "MX", 1.50),
        Row("0000001", "Fantastic Hostel", "2016-06-02 11:00", "US", 1.50)
      )
    )

    val bidsSchema = StructType(
      Array(StructField("MotelID", StringType, nullable = true),
        StructField("BidDate", StringType, nullable = true),
        StructField("LoSa", StringType, nullable = true),
        StructField("Price", DoubleType, nullable = true))
    )

    val motelsSchema = StructType(
      Array(StructField("MotelID", StringType, nullable = true),
        StructField("MotelName", StringType, nullable = true))
    )

    val expectedSchema = StructType(
      Array(StructField("MotelID", StringType, nullable = true),
        StructField("MotelName", StringType, nullable = true),
        StructField("BidDate", StringType, nullable = true),
        StructField("LoSa", StringType, nullable = true),
        StructField("Price", DoubleType, nullable = true))
    )

    val bids = sqlTestContext.createDataFrame(bidsData, bidsSchema)
    val motels = sqlTestContext.createDataFrame(motelsData, motelsSchema)

    val expected = sqlTestContext.createDataFrame(expectedData, expectedSchema)
    val enriched = MotelsHomeRecommendation.getEnriched(bids, motels)

    assertDataFrameEquals(expected, enriched)
  }

  //integration test
  @Test
  def shouldFilterErrorsAndCreateCorrectAggregates() = {

    runIntegrationTest()

    assertRddTextFiles(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    assertRddTextFiles(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))
  }

  @After
  def teardown(): Unit = {
    outputFolder.delete
  }

  private def runIntegrationTest() = {
    MotelsHomeRecommendation.processData(sqlTestContext, bidsPath = INPUT_BIDS_INTEGRATION, motelsPath = INPUT_MOTELS_INTEGRATION, exchangeRatesPath = INPUT_EXCHANGE_RATES_INTEGRATION, outputBasePath = outputFolder.getAbsolutePath)
  }

  private def assertRddTextFiles(expectedPath: String, actualPath: String) = {
    val expected = scTest.textFile(expectedPath)
    val actual = scTest.textFile(actualPath)
    RddComparator.printDiff(expected, actual)
  }

  private def getOutputPath(dir: String): String = {
    new Path(outputFolder.getAbsolutePath, dir).toString
  }
}

object MotelsHomeRecommendationTest {
  var scTest: SparkContext = null
  var sqlTestContext: HiveContext = null

  @BeforeClass
  def beforeTests() = {
    scTest = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("motels-home-recommendation test"))
    sqlTestContext = new HiveContext(scTest)
  }

  @AfterClass
  def afterTests() = {
    scTest.stop
  }

  // Contains data and schemas used in test,
  // methods to create necessary parquet sample files
  object Helper {

    //data as in bids_sample.txt
    val rawBidsData = scTest.parallelize(
      Seq(
        Row("0000008", "23-04-08-2016", "1.65", "0.91", "0.55", "0.76", "", "0.63"),
        Row("0000001", "06-05-02-2016", "ERROR_NO_BIDS_FOR_HOTEL", null, null, null, null, null),
        Row("0000004", "21-04-08-2016", "1.40", "", "2.04", "1.60", "1.79", "0.70"))
    )

    //schema for bid_sample.txt
    val rawBidSchema = StructType(
      Array(StructField("MotelID", StringType, nullable = true),
        StructField("BidDate", StringType, nullable = true),
        StructField("HU", StringType, nullable = true),
        StructField("UK", StringType, nullable = true),
        StructField("NL", StringType, nullable = true),
        StructField("US", StringType, nullable = true),
        StructField("MX", StringType, nullable = true),
        StructField("CA", StringType, nullable = true))
    )

    //data as in rates_sample.txt
    val ratesData = scTest.parallelize(
      Seq(
        Row("11-06-05-2016", "0.803"),
        Row("11-05-08-2016", "0.873"),
        Row("21-04-08-2016", "0.873"))
    )

    //schema for rates_sample.txt
    val ratesSchema = StructType(
      Array(StructField("date", StringType, nullable = true),
        StructField("rate", StringType, nullable = true))
    )

    //schema for motels_sample.txt
    val motelsSchema = StructType(
      Array(StructField("MotelID", StringType, nullable = true),
        StructField("MotelName", StringType, nullable = true),
        StructField("Country", StringType, nullable = true),
        StructField("URL", StringType, nullable = true),
        StructField("Comment", StringType, nullable = true))
    )

    //Creates bids_sample.parquet file from text file
    def createBidsParquetFromText(bidsTextPath: String, bidsParquetPath: String) = {

      val bidsRdd = scTest.textFile(bidsTextPath)
        .map(line => line.split(Constants.DELIMITER))
        .map(line => {
          if (line.length == 8) {
            Row(line(0), line(1), line(2), line(3), line(4), line(5), line(6), line(7))
          } else {
            Row(line(0), line(1), line(2), null, null, null, null, null, null)
          }
        })

      sqlTestContext.createDataFrame(bidsRdd, rawBidSchema).write.parquet(bidsParquetPath)
    }

    //Creates motels_sample.parquet file from text file
    def createMotelsParquetFromText(motelsTextPath: String, motelsParquetPath: String) = {

      val motelsRdd = scTest.textFile(motelsTextPath)
        .map(line => line.split(Constants.DELIMITER))
        .map(line => Row(line(0), line(1), line(2), line(3), line(4)))

      sqlTestContext.createDataFrame(motelsRdd, motelsSchema).write.parquet(motelsParquetPath)
    }

    //Deletes parquet file
    def deleteFile(path: String) = {
      import scala.reflect.io.Directory

      val directory = new Directory(new File(path))
      val deleted = directory.deleteRecursively()
    }
  }

}






