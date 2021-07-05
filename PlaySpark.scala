package guru.learningjournal.spark.examples





import java.util.Properties
import scala.language.implicitConversions
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter



object PlaySpark extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark_Practice")
      .master("local[3]")
      .getOrCreate()

    hemantExercise(spark)

    /*val inputDF= spark.read
      .option("header", "true")
      .option("inferfSchema", "true")
      .csv("data/products.csv")

    val salesDF= spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/sales.csv")

    val product_group_DF = salesDF.groupBy("product_id")
      .count()

    val filtered_product_group_DF = product_group_DF.where("count > 1")


    val date_group_DF =  salesDF.groupBy("date").agg(countDistinct(col("product_id")))


    //val filtered_product_group_DF = product_group_DF.where("Count_orders > 1")

    filtered_product_group_DF.show(10)

    date_group_DF.show(10)

    inputDF.printSchema()

    println(inputDF.count())*/


    // [START] Solution for https://jaceklaskowski.github.io/spark-workshop/exercises/sql/explode-structs-array.html
/*    val dayschema = new StructType()
      .add("close",StringType,true)
      .add("open",StringType,true)

    val hours_schema = new StructType()
      .add("Monday",dayschema,true)
      .add("Tuesday",dayschema,true)
      .add("Friday",dayschema,true)
      .add("Wednesday",dayschema,true)
      .add("Thursday",dayschema,true)
      .add("Sunday",dayschema,true)
      .add("Saturday",dayschema,true)


    val main_schema = new StructType()
      .add("business_id",StringType,true)
      .add("full_address",StringType,true)
      .add("hours",hours_schema,true)


    val inputJson = spark.read.schema(main_schema).option("multiline","true").
      json("E:\\CBS\\Spark_Learning\\SparkProgrammingInScala-master\\01-HelloSpark\\data\\Input.json") */







      //input.withColumn("Exploded_hours",from_json(col("hours"),hours_schema))

      //.select("Exploded_hours.*")
    // [END] https://jaceklaskowski.github.io/spark-workshop/exercises/sql/explode-structs-array.html


   /* val input = spark.read.option("multiline","true").
      json("E:\\CBS\\Spark_Learning\\SparkProgrammingInScala-master\\01-HelloSpark\\data\\Input1.json")

    input.printSchema()

      val flatten = input.select(col("*"), explode(col("Properties")) as "SubContent")


    val flatten_pivot = flatten.groupBy("ProductNum","unitCount")
      .pivot("Subcontent.key")
      .agg(first("SubContent.value"))

    flatten_pivot.show() */


    // [START] Solution for https://jaceklaskowski.github.io/spark-workshop/exercises/sql/Finding-Ids-of-Rows-with-Word-in-Array-Column.html
    /*val input = Seq(
     (1,"one,two,three"),
      (2,"four,one,five"),
    (3,"seven,nine,one,two"),
      (4,"two,three,five"),
        (5,"six,five,one"))

    val inputDF = spark.createDataFrame(input).toDF("id","words")

    val splitted_inputDF = inputDF.withColumn("words_array",split(col("words"),","))

    splitted_inputDF.show()
    logger.info(splitted_inputDF.schema)

    val flattened_splitted_inputDF = splitted_inputDF
      .select(col("*"),explode(col("words_array")) as "Exploded_words_array")
      .drop("words","words_array")

    val grouped = flattened_splitted_inputDF.groupBy("Exploded_words_array")
      .agg(collect_list(col("id")))

    logger.info("Schema here:"+grouped.printSchema()) */

    // [END] Solution for https://jaceklaskowski.github.io/spark-workshop/exercises/sql/Finding-Ids-of-Rows-with-Word-in-Array-Column.html



    // [START] Very good demo example of flatmap and map operator
   /* val arrayStructureData = Seq(
      Row("James,,Smith",List("Java","Scala","C++"),"CA"),
      Row("Michael,Rose,",List("Spark","Java","C++"),"NJ"),
      Row("Robert,,Williams",List("CSharp","VB","R"),"NV")
    )

    val arrayStructureSchema = new StructType()
      .add("name",StringType)
      .add("languagesAtSchool", ArrayType(StringType))
      .add("currentState", StringType)

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
    import spark.implicits._


    df.foreach(f => logger.info("Check individual fields "+ f.getString(0) +"::"+f.getSeq(1) + ":"+ f.getString(2)   ))
    df.foreach(f => logger.info("Check o/p "+ f.getSeq[String](1).map((f.getString(0),_,f.getString(2)))))

    //flatMap() Usage
    val df2=df.flatMap(f=> f.getSeq[String](1).map((f.getString(0),_,f.getString(2))))
      .toDF("Name","language","State") */

    // [END] Very good demo example of flatmap and map operator

    // [START] -  https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Flattening-Array-Columns-From-Datasets-of-Arrays-to-Datasets-of-Array-Elements.html
    /*val input = Seq(
      Seq("a","b","c"),
      Seq("X","Y","Z"),
      Seq("p","q","r")).toDF("num")



    val input_map = input.map(f => {
      f.getSeq[String](0).zipWithIndex
    })




   val expl_input_map = input_map.withColumn("Exploded",explode(col("value")))


    val expl_input_map1 = expl_input_map.withColumn("Element",col("Exploded").getItem("_1"))
      .withColumn("ElementIndex",col("Exploded").getItem("_2"))

    val expl_input_map2 = expl_input_map1.drop("value","Exploded") */

    // [END] https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Flattening-Array-Columns-From-Datasets-of-Arrays-to-Datasets-of-Array-Elements.html


    // [START] https://jaceklaskowski.github.io/spark-workshop/exercises/sql/Finding-Most-Populated-Cities-Per-Country.html

    /*val input_city_data= spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/City_data.csv")

    input_city_data.printSchema()

    val input_city_data_converted = input_city_data.withColumn("population_number",regexp_replace(col("population")," ","").cast(IntegerType))

    val input_city_data_grouped = input_city_data_converted.groupBy("country")
      .max("population_number")

    val input_city_data_grouped1 = input_city_data_grouped.withColumnRenamed("max(population_number)","population_number_grouped")
      .withColumnRenamed("country","country_grouped")

    input_city_data_grouped1.printSchema()

    val joinExpr = input_city_data_converted.col("country") === input_city_data_grouped1.col("country_grouped") && input_city_data_converted.col("population_number") === input_city_data_grouped1.col("population_number_grouped")

    val joinType = "inner"

    val output = input_city_data_converted.join(input_city_data_grouped1, joinExpr, joinType)

    

    output.show()

    import scala.io.StdIn.readLine
    val inp1 = readLine()*/

    // [END] https://jaceklaskowski.github.io/spark-workshop/exercises/sql/Finding-Most-Populated-Cities-Per-Country.html

    // [START] https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Difference-in-Days-Between-Dates-As-Strings.html
    /*val dates = Seq(
      "08/11/2015",
      "09/11/2015",
      "09/12/2015").toDF("date_string")

    val actual_dates = dates.withColumn("to_date",to_date(col("date_string"),"dd/MM/yyyy"))

    val date_diff = actual_dates.withColumn("diff",datediff(current_date(),col("to_date")))

    date_diff.show()*/

    //[END] https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Difference-in-Days-Between-Dates-As-Strings.html

    // [START] https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-counting-occurences-of-years-and-months-for-past-24-months.html#input-dataset
  /*  val endDate = LocalDate.now()

    val startDate = LocalDate.now().minusYears(2)

    def dayIterator(start: LocalDate, end: LocalDate) = Iterator.iterate(start)(_ plusDays 1) takeWhile (_ isBefore end)

    val dateList = dayIterator(startDate,endDate).toList.map(f => {
      f.format(DateTimeFormatter.ofPattern("yyyyMM"))
    }).distinct

    val dateList_map = dateList.map(f =>(f,0)).toDF("year_month","amount")


    val InputSales = Seq(("202001",1100),
                         ("201912",100),
                          ("201910",100),
                          ("201909",400),
                          ("201601",5000)).toDF("year_month_inp","amount_inp")

    val joinExpr1 = dateList_map.col("year_month") === InputSales.col("year_month_inp")

    val joinType1 = "left"

    val outputMonths = dateList_map.join(InputSales, joinExpr1, joinType1)
      .select(col("year_month"),when(col("amount_inp") isNotNull ,col("amount_inp"))
        .when(col("amount_inp") isNull , col("amount"))
        .otherwise("Unknown").alias("final_amount"))

    outputMonths.show() */

    // [END] https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-counting-occurences-of-years-and-months-for-past-24-months.html#input-dataset


    // [START] https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Why-are-all-fields-null-when-querying-with-schema.html

    /*val DateIpSchema = new StructType()
      .add("TimeStamp",StringType,true)
      .add("ip_address",StringType,true)

    val DateIpDF= spark.read
      .schema(DateIpSchema)
      .option("header", "false")
      .option("delimiter","|")
      .csv("data/DateIp.csv")



    val DateIpDF_totimestamp = DateIpDF.withColumn("ConvertedToTimeStamp", to_timestamp(regexp_replace(col("TimeStamp"),",","."),"yyyy-MM-dd HH:mm:ss.SSS" ) )

    DateIpDF_totimestamp.show(false) */

    // [END] https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Why-are-all-fields-null-when-querying-with-schema.html

    // [START] https://jaceklaskowski.github.io/spark-workshop/exercises/sql/How-to-add-days-as-values-of-a-column-to-date.html
    /*val data = Seq(
      (0, "2016-01-1"),
      (1, "2016-02-2"),
      (2, "2016-03-22"),
      (3, "2016-04-25"),
      (4, "2016-05-21"),
      (5, "2016-06-1"),
      (6, "2016-03-21")
    ).toDF("number_of_days", "date")

    val data_futureDate = data.withColumn("FutureDate",date_add(col("date"),col("number_of_days")))

    data_futureDate.show()*/

    // [END] https://jaceklaskowski.github.io/spark-workshop/exercises/sql/How-to-add-days-as-values-of-a-column-to-date.html

    // [START] https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Using-UDFs.html
    /*val sampleUDFTest = Seq((1,"John"),
      (2,"Peter"),
      (3,"Mathew")).toDF("id","EmpName")

    sampleUDFTest.createOrReplaceTempView("Employee")



    val myUdf = udf(callUDF(_:String):String)

    spark.udf.register("callUDF", callUDF(_:String):String)

    val sampleUDFTest_upper = spark.sql("select id,callUDF(EmpName) from Employee")

    sampleUDFTest_upper.show(false) */

    // [END] https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Using-UDFs.html

    //[START] https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Using-pivot-to-generate-a-single-row-matrix.html
   /* val pivotDemoInput = Seq((20090622,458),
                       (20090624,31068),
        (20090626,151),
          (20090629,148),
            (20090914,453)).toDF("update","cc")



    val pivotDemoInput_group = pivotDemoInput.groupBy()
      .pivot("update")
      .agg(max(col("cc")))

    pivotDemoInput_group.show(false) */

    //[END] https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Using-pivot-to-generate-a-single-row-matrix.html

    // [START] https://jaceklaskowski.github.io/spark-workshop/exercises/sql/Using-pivot-for-Cost-Average-and-Collecting-Values.html
    /*val pivotData1 = Seq(
      (0, "A", 223, "201603", "PORT"),
      (0, "A", 22, "201602", "PORT"),
      (0, "A", 422, "201601", "DOCK"),
      (1, "B", 3213, "201602", "DOCK"),
      (1, "B", 3213, "201601", "PORT"),
      (2, "C", 2321, "201601", "DOCK")
    ).toDF("id","type", "cost", "date", "ship")

    val pivotData1_pivotted = pivotData1.groupBy("id","type")
      .pivot("date")
      .agg(first(col("cost")))

    val pivotData2_pivotted = pivotData1.groupBy("id","type")
      .pivot("date")
      .agg(collect_list(col("ship")))

    pivotData1_pivotted.show(false)
    pivotData2_pivotted.show(false)
     */

    // [END] https://jaceklaskowski.github.io/spark-workshop/exercises/sql/Using-pivot-for-Cost-Average-and-Collecting-Values.html

  }

  def callUDF(s:String):String={
       s.toUpperCase()
  }

  /*
  def fill(trades, prices):
    """
    Combine the sets of events and fill forward the value columns so that each
    row has the most recent non-null value for the corresponding id. For
    example, given the above input tables the expected output is:

    +---+-------------+-----+-----+-----+--------+
    | id|    timestamp|  bid|  ask|price|quantity|
    +---+-------------+-----+-----+-----+--------+
    | 10|1546300799000| 37.5|37.51| null|    null|
    | 10|1546300800000| 37.5|37.51| 37.5|   100.0|
    | 10|1546300801000| 37.5|37.51|37.51|   100.0|
    | 10|1546300802000|37.51|37.52|37.51|   100.0|
    | 20|1546300804000| null| null|12.67|   300.0|
    | 10|1546300806000| 37.5|37.51|37.51|   100.0|
    | 10|1546300807000| 37.5|37.51| 37.5|   200.0|
    +---+-------------+-----+-----+-----+--------+

    :param trades: DataFrame of trade events
    :param prices: DataFrame of price events
    :return: A DataFrame of the combined events and filled.
    """
    raise NotImplementedError()


def pivot(trades, prices):
    """
    Pivot and fill the columns on the event id so that each row contains a
    column for each id + column combination where the value is the most recent
    non-null value for that id. For example, given the above input tables the
    expected output is:

    +---+-------------+-----+-----+-----+--------+------+------+--------+-----------+------+------+--------+-----------+
    | id|    timestamp|  bid|  ask|price|quantity|10_bid|10_ask|10_price|10_quantity|20_bid|20_ask|20_price|20_quantity|
    +---+-------------+-----+-----+-----+--------+------+------+--------+-----------+------+------+--------+-----------+
    | 10|1546300799000| 37.5|37.51| null|    null|  37.5| 37.51|    null|       null|  null|  null|    null|       null|
    | 10|1546300800000| null| null| 37.5|   100.0|  37.5| 37.51|    37.5|      100.0|  null|  null|    null|       null|
    | 10|1546300801000| null| null|37.51|   100.0|  37.5| 37.51|   37.51|      100.0|  null|  null|    null|       null|
    | 10|1546300802000|37.51|37.52| null|    null| 37.51| 37.52|   37.51|      100.0|  null|  null|    null|       null|
    | 20|1546300804000| null| null|12.67|   300.0| 37.51| 37.52|   37.51|      100.0|  null|  null|   12.67|      300.0|
    | 10|1546300806000| 37.5|37.51| null|    null|  37.5| 37.51|   37.51|      100.0|  null|  null|   12.67|      300.0|
    | 10|1546300807000| null| null| 37.5|   200.0|  37.5| 37.51|    37.5|      200.0|  null|  null|   12.67|      300.0|
    +---+-------------+-----+-----+-----+--------+------+------+--------+-----------+------+------+--------+-----------+

    :param trades: DataFrame of trade events
    :param prices: DataFrame of price events
    :return: A DataFrame of the combined events and pivoted columns.
    """
    raise NotImplementedError()
  
  */
  
  /*Problem statement given above */
  def hemantExercise(spark:SparkSession):Unit={

    import spark.implicits._

    val trades = Seq ((10, 1546300800000L, 37.50, 100.000),
        (10, 1546300801000L, 37.51, 100.000),
        (20, 1546300804000L, 12.67, 300.000),
        (10, 1546300807000L, 37.50, 200.000)).toDF("id","timestamp","price","quantity")

    val prices = Seq((10, 1546300799000L, 37.50, 37.51),
      (10, 1546300802000L, 37.51, 37.52),
      (10, 1546300806000L, 37.50, 37.51)).toDF("id","timestamp","bid","ask")


    val prices_new = prices.withColumnRenamed("id","prices_id")
      .withColumnRenamed("timestamp","prices_timestamp")

    val joinExpr = trades.col("timestamp") === prices_new.col("prices_timestamp")
    val joinType = "full"

    val out = trades.join(prices_new,joinExpr,joinType)

    val out1 = out.withColumn("id1",when(col("id").isNull,col("prices_id"))
    .otherwise(col("id")))
      .withColumn("timestamp1",when(col("timestamp").isNull,col("prices_timestamp"))
      .otherwise(col("timestamp")))
      .drop("id").drop("timestamp").drop("prices_id").drop("prices_timestamp")
      .withColumnRenamed("id1","id")
      .withColumnRenamed("timestamp1","timestamp").orderBy("timestamp")

    val window1 = Window.partitionBy("id").orderBy("timestamp")



    val out2 = out1.withColumn("bid",
      when(col("bid").isNull,last("bid",true).over(window1))
    .otherwise(col("bid")))
      .withColumn("ask",when(col("ask").isNull,last("ask",true).over(window1))
        .otherwise(col("ask")))
      .withColumn("price",when(col("price").isNull,last("price",true).over(window1))
        .otherwise(col("price")))
      .withColumn("quantity",when(col("quantity").isNull,last("quantity",true).over(window1))
        .otherwise(col("quantity"))).orderBy("timestamp")
      .select("id","timestamp","bid","ask","price","quantity")

    val union1 = trades.withColumn("bid",lit(null))
      .withColumn("ask",lit(null))
      .unionByName(prices.withColumn("price",lit(null))
        .withColumn("quantity",lit(null)))



    val out3 = out2.groupBy("id","timestamp")
      .pivot("id")
      .agg(max(col("bid")),max(col("ask")),max(col("price")),max(col("quantity")))

    val window2 = Window.orderBy("timestamp")

    out3.withColumn("10_max(bid)",
      when(col("10_max(bid)").isNull,lag("10_max(bid)",1).over(window2))
        .otherwise(col("10_max(bid)")))
      .withColumn("10_max(ask)",
        when(col("10_max(ask)").isNull,lag("10_max(ask)",1).over(window2))
          .otherwise(col("10_max(ask)")))
      .withColumn("20_max(bid)",
        when(col("20_max(bid)").isNull,lag("20_max(bid)",1).over(window2))
          .otherwise(col("20_max(bid)")))
      .withColumn("20_max(price)",
        when(col("20_max(price)").isNull,last("20_max(price)",true).over(window2))
          .otherwise(col("20_max(price)")))
      .withColumn("20_max(quantity)",
        when(col("20_max(quantity)").isNull,last("20_max(quantity)",true).over(window2))
          .otherwise(col("20_max(quantity)")))
      .orderBy("timestamp")
      .show(false)

  }


}
