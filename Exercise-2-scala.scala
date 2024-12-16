// Databricks notebook source
// MAGIC %md
// MAGIC # COMP.CS.320 Data-Intensive Programming, Exercise 2
// MAGIC
// MAGIC This exercise contains basic tasks of data processing using Spark and DataFrames.
// MAGIC
// MAGIC This is the **Scala** version, switch to the Python version if you want to do the tasks in Python.
// MAGIC
// MAGIC Each task has its own cell for the code. Add your solutions to the cells. You are free to add more cells if you feel it is necessary. There are cells with example outputs following each task.
// MAGIC
// MAGIC Don't forget to submit your solutions to Moodle.

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Some resources that can help with the tasks in this exercise:
// MAGIC
// MAGIC - The [tutorial notebook](https://adb-7895492183558578.18.azuredatabricks.net/?o=7895492183558578#notebook/2974598884121429) from our course
// MAGIC - Chapter 3 in [Learning Spark, 2nd Edition](https://learning.oreilly.com/library/view/learning-spark-2nd/9781492050032/)
// MAGIC     - There are additional code examples in the related [GitHub repository](https://github.com/databricks/LearningSparkV2).
// MAGIC     - The book related notebooks can be imported to Databricks by choosing `import` in your workspace and using the URL<br> `https://github.com/databricks/LearningSparkV2/blob/master/notebooks/LearningSparkv2.dbc`
// MAGIC - [Databricks tutorial](https://docs.databricks.com/en/getting-started/dataframes.html) of using Spark DataFrames
// MAGIC - [Apache Spark documentation](https://spark.apache.org/docs/3.5.0/sql-ref-functions.html) on all available functions that can be used on DataFrames.<br>
// MAGIC   The full [Spark Scala functions API listing](https://spark.apache.org/docs/3.5.0/api/scala/org/apache/spark/sql/functions$.html) for the functions package might have some additional functions listed that have not been updated in the documentation.

// COMMAND ----------

// some imports that might be required in the tasks

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 1 - Create DataFrame
// MAGIC
// MAGIC As mentioned in the [tutorial notebook](https://adb-7895492183558578.18.azuredatabricks.net/?o=7895492183558578#notebook/2974598884121429), Azure Storage Account and Azure Data Lake Storage Gen2 are used in the course to provide a place to read and write data files.
// MAGIC In the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2024-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2024gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) in the `exercises/ex2` folder is file `nordics_weather.csv` that contains weather data from Finland, Sweden, and Norway in CSV format.
// MAGIC
// MAGIC The data is based on a dataset from Kaggle: [Finland, Norway, and Sweden Weather Data 2015-2019](https://www.kaggle.com/datasets/adamwurdits/finland-norway-and-sweden-weather-data-20152019).
// MAGIC The Kaggle page has further descriptions on the data and the units used in the data.
// MAGIC
// MAGIC Read the data from the CSV file into DataFrame called `weatherDF`. Let Spark infer the schema for the data.
// MAGIC Note that the column separator in the CSV file is a semicolon (`;`) instead of the default comma.
// MAGIC
// MAGIC Print out the schema.
// MAGIC Study the schema and compare it to the data in the CSV file. Do they match?

// COMMAND ----------

val weatherDF: DataFrame = spark.read
                          .option("header", "true")
                          .option("sep", ";")
                          .option("inferSchema", "true")
                          .csv("abfss://shared@tunics320f2024gen2.dfs.core.windows.net/exercises/ex2/nordics_weather.csv")

// code that prints out the schema for weatherDF
weatherDF.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Example output for task 1:
// MAGIC
// MAGIC ```text
// MAGIC root
// MAGIC  |-- country: string (nullable = true)
// MAGIC  |-- date: date (nullable = true)
// MAGIC  |-- temperature_avg: double (nullable = true)
// MAGIC  |-- temperature_min: double (nullable = true)
// MAGIC  |-- temperature_max: double (nullable = true)
// MAGIC  |-- precipitation: double (nullable = true)
// MAGIC  |-- snow_depth: double (nullable = true)
// MAGIC  ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 2 - The first items from DataFrame
// MAGIC
// MAGIC In this task and all the following tasks you can (and should) use the variables defined in the previous tasks.
// MAGIC
// MAGIC Part 1:
// MAGIC
// MAGIC - Fetch the first **seven** rows of the weather data frame and print their contents.
// MAGIC
// MAGIC Part 2:
// MAGIC
// MAGIC - Fetch the last **six** rows of the weather data frame, but this time only include the `country`, `date`, and `temperature_avg` columns.
// MAGIC - Print out the result.

// COMMAND ----------

val weatherSample1: Array[Row] = weatherDF.take(7)

println("The first seven rows of the weather data frame:")
weatherSample1.foreach(row => println(row))
println("==============================")

val weatherSample2: Array[Row] = weatherDF.select("country", "date", "temperature_avg").tail(6)

println("The last six rows of the weather data frame:")
weatherSample2.foreach(row => println(row))
println("==============================")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Example output for task 2:
// MAGIC
// MAGIC ```text
// MAGIC The first seven rows of the weather data frame:
// MAGIC [Finland,2019-12-28,-9.107407407,-15.28888889,-4.703947368,0.789265537,116.4210526]
// MAGIC [Finland,2015-04-08,4.025,1.336129032,6.196129032,0.116666667,486.5833333]
// MAGIC [Sweden,2018-10-20,5.077777778,1.241743119,9.210550459,0.885153584,0.0]
// MAGIC [Finland,2016-03-07,-0.775,-2.065584416,0.001315789,2.122613065,469.6315789]
// MAGIC [Sweden,2017-11-29,-1.355555556,-7.81146789,-3.817889908,2.728667791,103.3424658]
// MAGIC [Finland,2016-12-24,-1.275,-5.344736842,0.930263158,4.751041667,214.8181818]
// MAGIC [Norway,2019-12-29,2.657894737,0.575,6.792307692,19.75630252,195.8148148]
// MAGIC ==============================
// MAGIC The last six rows of the weather data frame:
// MAGIC [Norway,2015-02-21,-2.742105263]
// MAGIC [Norway,2019-11-19,1.315789474]
// MAGIC [Finland,2015-12-09,2.517857143]
// MAGIC [Norway,2017-05-21,7.710526316]
// MAGIC [Sweden,2015-07-28,14.36]
// MAGIC [Norway,2018-02-10,-0.131578947]
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 3 - Minimum and maximum
// MAGIC
// MAGIC Find the minimum temperature and the maximum temperature from the whole data.

// COMMAND ----------

val minTemp: Double = weatherDF.select(min("temperature_min")).first.getDouble(0)
val maxTemp: Double = weatherDF.select(max("temperature_max")).first.getDouble(0)

println(s"Min temperature is $minTemp")
println(s"Max temperature is $maxTemp")
println("==============================")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Example output for task 3:
// MAGIC
// MAGIC ```text
// MAGIC Min temperature is -29.63961039
// MAGIC Max temperature is 30.56143791
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 4 - Adding a column
// MAGIC
// MAGIC Add a new column `year` to the weatherDataFrame and print out the schema for the new DataFrame.
// MAGIC
// MAGIC The type of the new column should be integer and value calculated from column "date".
// MAGIC

// COMMAND ----------

val weatherDFWithYear: DataFrame = weatherDF.withColumn("year", year(col("date")))

// code that prints out the schema for weatherDFWithYear
weatherDFWithYear.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Example output for task 4:
// MAGIC
// MAGIC ```text
// MAGIC root
// MAGIC  |-- country: string (nullable = true)
// MAGIC  |-- date: date (nullable = true)
// MAGIC  |-- temperature_avg: double (nullable = true)
// MAGIC  |-- temperature_min: double (nullable = true)
// MAGIC  |-- temperature_max: double (nullable = true)
// MAGIC  |-- precipitation: double (nullable = true)
// MAGIC  |-- snow_depth: double (nullable = true)
// MAGIC  |-- year: integer (nullable = true)
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 5 - Aggregated DataFrame 1
// MAGIC
// MAGIC Find the minimum and the maximum temperature for each year.
// MAGIC
// MAGIC Sort the resulting DataFrame based on year so that the latest year is the first row in the DataFrame.

// COMMAND ----------

val temperatureDF: DataFrame = weatherDFWithYear
  .groupBy("year")
  .agg(
    min("temperature_min").as("min_temperature"),
    max("temperature_max").as("max_temperature")
  )
  .sort(desc("year"))

temperatureDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Example output for task 5:
// MAGIC
// MAGIC ```text
// MAGIC +----+---------------+---------------+
// MAGIC |year|temperature_min|temperature_max|
// MAGIC +----+---------------+---------------+
// MAGIC |2019|   -26.63708609|    29.47627907|
// MAGIC |2018|   -24.00592105|    30.56143791|
// MAGIC |2017|        -24.922|    23.14771242|
// MAGIC |2016|   -29.63961039|    26.28026906|
// MAGIC |2015|   -21.97961783|     25.7285124|
// MAGIC +----+---------------+---------------+
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 6 - Aggregated DataFrame 2
// MAGIC
// MAGIC Expanding from task 5, create a DataFrame that separates the data by both year and country.
// MAGIC
// MAGIC For each year and country pair, the resulting DataFrame should contain the following values:
// MAGIC
// MAGIC - the number of entries (as in rows in the original data) there are for that year
// MAGIC - the minimum temperature (rounded to 1 decimal precision)
// MAGIC - the maximum temperature (rounded to 1 decimal precision)
// MAGIC - the average snow depth (rounded to whole numbers)
// MAGIC
// MAGIC Order the DataFrame first by year with the latest year first, and then by country using alphabetical ordering.

// COMMAND ----------

val task6DF: DataFrame = weatherDFWithYear
  .groupBy("year", "country")
  .agg(
    count("country").as("entries"),
    round(min("temperature_min"), 1).as("temperature_min"),
    round(max("temperature_max"), 1).as("temperature_max"),
    round(avg("snow_depth")).cast("int").as("snow_depth_avg")
  )
  .sort(desc("year"), asc("country"))

task6DF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Example output for task 6:
// MAGIC
// MAGIC ```text
// MAGIC +----+-------+-------+---------------+---------------+--------------+
// MAGIC |year|country|entries|temperature_min|temperature_max|snow_depth_avg|
// MAGIC +----+-------+-------+---------------+---------------+--------------+
// MAGIC |2019|Finland|    365|          -26.6|           28.7|           159|
// MAGIC |2019| Norway|    365|          -12.4|           26.0|           108|
// MAGIC |2019| Sweden|    365|          -17.3|           29.5|            81|
// MAGIC |2018|Finland|    365|          -24.0|           30.6|           178|
// MAGIC |2018| Norway|    365|          -13.3|           27.9|           158|
// MAGIC |2018| Sweden|    365|          -19.8|           29.8|           136|
// MAGIC |2017|Finland|    365|          -24.9|           23.1|           218|
// MAGIC |2017| Norway|    365|          -13.3|           21.1|            89|
// MAGIC |2017| Sweden|    365|          -21.9|           21.6|            72|
// MAGIC |2016|Finland|    366|          -29.6|           25.0|           173|
// MAGIC |2016| Norway|    366|          -14.5|           23.6|            96|
// MAGIC |2016| Sweden|    366|          -22.4|           26.3|            71|
// MAGIC |2015|Finland|    365|          -22.0|           23.8|           186|
// MAGIC |2015| Norway|    365|          -11.1|           22.3|           114|
// MAGIC |2015| Sweden|    365|          -16.1|           25.7|            71|
// MAGIC +----+-------+-------+---------------+---------------+--------------+
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 7 - Aggregated DataFrame 3
// MAGIC
// MAGIC Using the DataFrame created in task 6, `task6DF`, find the following values:
// MAGIC
// MAGIC - the minimum temperature in Finland for year 2016
// MAGIC - the maximum temperature in Sweden for year 2017
// MAGIC - the difference between the maximum and the minimum temperature in Norway for year 2018
// MAGIC - the average snow depth for year 2015 when taking into account all three countries

// COMMAND ----------

val min2016: Double = task6DF
  .filter($"year" === 2016)
  .filter($"country" === "Finland")
  .select("temperature_min").first.getDouble(0)

val max2017: Double = task6DF
  .filter($"year" === 2017)
  .filter($"country" === "Sweden")
  .select("temperature_max").first.getDouble(0)

val difference2018: Double = {
  val row = task6DF
    .filter($"year" === 2018)
    .filter($"country" === "Norway")
    .select("temperature_min", "temperature_max").first
  row.getDouble(1) - row.getDouble(0)
}

val snow2015: Double = task6DF
  .filter($"year" === 2015)
  .agg(avg("snow_depth_avg")).first.getDouble(0)

println(s"Min temperature (Finland, 2016):       ${min2016} °C")
println(s"Max temperature (Sweden, 2017):         ${max2017} °C")
println(s"Temperature difference (Norway, 2018):  ${difference2018} °C")
println(s"The average snow depth (2015):          ${Math.round(snow2015)} mm")
println("==============================")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Example output for task 7:
// MAGIC
// MAGIC ```text
// MAGIC Min temperature (Finland, 2016):       -29.6 °C
// MAGIC Max temperature (Sweden, 2017):         21.6 °C
// MAGIC Temperature difference (Norway, 2018):  41.2 °C
// MAGIC The average snow depth (2015):          124 mm
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 8 - One more aggregated DataFrame task
// MAGIC
// MAGIC Part 1:
// MAGIC
// MAGIC - How many days in each year was the average temperature below -10 °C in Finland?
// MAGIC - How many days in total for each country was the average temperature above +5 °C when snow depth was above 100 mm?
// MAGIC
// MAGIC Part 2:
// MAGIC
// MAGIC - What are the top 10 days in Finland on which the difference between the maximum and the minimum temperature within the day was the largest?

// COMMAND ----------

val daysBelowMinus10DF: DataFrame = weatherDFWithYear
.filter($"country" === "Finland")
.groupBy("year")
.agg(
    count(when($"temperature_avg" < -10, true)).as("count")
  )
.sort(asc("year"))

println("The number of days the average temperature in Finland was below -10 °C:")
daysBelowMinus10DF.show()

val daysAbove5DF: DataFrame = weatherDFWithYear
.groupBy("country")
.agg(
  count(when($"temperature_avg" > 5 && $"snow_depth" > 100, true)).as("count")
)
.sort(asc("country"))

println("The number of days the temperature in each country was above +5 °C when snow depth was above 100 mm:")
daysAbove5DF.show()


val differenceDaysDF: DataFrame = weatherDFWithYear
.filter($"country" === "Finland")
.withColumn("temperature_diff",
round($"temperature_max" - $"temperature_min", 2))
.select("date", "temperature_diff")
.sort(desc("temperature_diff"))
.limit(10)

println("The top 10 days in Finland with the largest temperature difference:")
differenceDaysDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Example output for task 8:
// MAGIC
// MAGIC ```text
// MAGIC The number of days the average temperature in Finland was below -10 °C:
// MAGIC +----+-----+
// MAGIC |year|count|
// MAGIC +----+-----+
// MAGIC |2015|   13|
// MAGIC |2016|   26|
// MAGIC |2017|   10|
// MAGIC |2018|   38|
// MAGIC |2019|   27|
// MAGIC +----+-----+
// MAGIC
// MAGIC The number of days the temperature in each country was above +5 °C when snow depth was above 100 mm:
// MAGIC +-------+-----+
// MAGIC |country|count|
// MAGIC +-------+-----+
// MAGIC |Finland|   75|
// MAGIC | Norway|   10|
// MAGIC | Sweden|   42|
// MAGIC +-------+-----+
// MAGIC
// MAGIC The top 10 days in Finland with the largest temperature difference:
// MAGIC +----------+----------------+
// MAGIC |      date|temperature_diff|
// MAGIC +----------+----------------+
// MAGIC |2018-05-13|           17.94|
// MAGIC |2018-05-14|           17.07|
// MAGIC |2018-05-12|           16.61|
// MAGIC |2018-05-11|           16.52|
// MAGIC |2018-05-25|           16.42|
// MAGIC |2018-05-15|           16.39|
// MAGIC |2018-05-16|           15.99|
// MAGIC |2016-01-22|           15.98|
// MAGIC |2018-05-28|           15.98|
// MAGIC |2019-04-18|           15.74|
// MAGIC +----------+----------------+
// MAGIC ```
