// Databricks notebook source
// MAGIC %md
// MAGIC # COMP.CS.320 Data-Intensive Programming, Exercise 1
// MAGIC
// MAGIC This exercise is mostly introduction to the Azure Databricks notebook system.
// MAGIC
// MAGIC There are some basic programming tasks that can be done in either Scala or Python. The final two tasks are very basic Spark related tasks.
// MAGIC
// MAGIC This is the **Scala** version, switch to the Python version if you want to do the tasks in Python.
// MAGIC
// MAGIC Each task has its own cell(s) for the code. Add your solutions to the cells. You are free to add more cells if you feel it is necessary. There are cells with test code or example output following most of the tasks that involve producing code.
// MAGIC
// MAGIC Don't forget to submit your solutions to Moodle.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 1 - Read tutorial
// MAGIC
// MAGIC Read the "[Basics of using Databricks notebooks](https://adb-7895492183558578.18.azuredatabricks.net/?o=7895492183558578#notebook/2974598884121429)" tutorial notebook.
// MAGIC Clone the tutorial notebook to your own workspace and run at least the first couple code examples.
// MAGIC
// MAGIC To get a point from this task, add "done" (or something similar) to the following cell (after you have read the tutorial).

// COMMAND ----------

// MAGIC %md
// MAGIC Task 1 is done

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 2 - Basic function
// MAGIC
// MAGIC Part 1:
// MAGIC
// MAGIC - Write a simple function `mySum` that takes two integer as parameters and returns their sum.
// MAGIC
// MAGIC Part 2:
// MAGIC
// MAGIC - Write a function `myTripleSum` that takes three integers as parameters and returns their sum.

// COMMAND ----------

def mySum(a: Int, b: Int): Int = a + b

def myTripleSum(a: Int, b: Int, c: Int): Int = a + b + c

// COMMAND ----------

// you can test your functions by running both the previous and this cell

val sum41 = mySum(20, 21)
sum41 == 41 match {
    case true => println(s"mySum: correct result: 20+21 = ${sum41}")
    case false => println(s"mySum: wrong result: ${sum41} != 41")
}
val sum65 = myTripleSum(20, 21, 24)
sum65 == 65 match {
    case true => println(s"myTripleSum: correct result: 20+21+24 = ${sum65}")
    case false => println(s"myTripleSum: wrong result: ${sum65} != 65")
}

println("==============================")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 3 - Fibonacci numbers
// MAGIC
// MAGIC The Fibonacci numbers, `F_n`, are defined such that each number is the sum of the two preceding numbers. The first two Fibonacci numbers are:
// MAGIC
// MAGIC $$F_0 = 0 \qquad F_1 = 1$$
// MAGIC
// MAGIC In the following cell, write a **recursive** function, `fibonacci`, that takes in the index and returns the Fibonacci number. (no need for any optimized solution here)
// MAGIC

// COMMAND ----------

def fibonacci(n: Int): Int = {
    if ( n <= 1 ) n
    else fibonacci(n-1) + fibonacci(n-2)
}

// COMMAND ----------

val fibo6 = fibonacci(6)
fibo6 == 8 match {
    case true => println("correct result: fibonacci(6) == 8")
    case false => println(s"wrong result: ${fibo6} != 8")
}

val fibo11 = fibonacci(11)
fibo11 == 89 match {
    case true => println("correct result: fibonacci(11) == 89")
    case false => println(s"wrong result: ${fibo11} != 89")
}

println("==============================")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 4 - Higher order functions 1
// MAGIC
// MAGIC - `map` function can be used to transform the elements of a list.
// MAGIC - `reduce` function can be used to combine the elements of a list.
// MAGIC
// MAGIC Part 1:
// MAGIC
// MAGIC - Using the `myList`as a starting point, use function `map` to calculate the cube of each element, and then use the reduce function to calculate the sum of the cubes.
// MAGIC
// MAGIC Part 2:
// MAGIC
// MAGIC - Using functions `map` and `reduce`, find the largest value for f(x)=1+9*x-x^2 when the input values x are the values from `myList`.

// COMMAND ----------

val myList: List[Int] = List(2, 3, 5, 7, 11, 13, 17, 19)

val cubeSum: Int = myList.map(x => x*x*x).reduceLeft(_ + _)

val largestValue: Int = myList.map(x => 1+9*x-x*x).reduceLeft((a, b) => if (a > b) a else b)

println(s"Sum of cubes:                    ${cubeSum}")
println(s"Largest value of f(x)=1+9*x-x^2:    ${largestValue}")
println("==============================")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Example output:
// MAGIC
// MAGIC ```text
// MAGIC Sum of cubes:                    15803
// MAGIC Largest value of f(x)=1+9*x-x^2:    21
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 5 - Higher order functions 2
// MAGIC
// MAGIC Explain the following code snippet. You can try the snippet piece by piece in a notebook cell or search help from Scaladoc ([https://www.scala-lang.org/api/2.12.x/](https://www.scala-lang.org/api/2.12.x/)).
// MAGIC
// MAGIC ```scala
// MAGIC "sheena is a punk rocker she is a punk punk"
// MAGIC     .split(" ")
// MAGIC     .map(s => (s, 1))
// MAGIC     .groupBy(p => p._1)
// MAGIC     .mapValues(v => v.length)
// MAGIC ```
// MAGIC
// MAGIC What about?
// MAGIC
// MAGIC ```scala
// MAGIC "sheena is a punk rocker she is a punk punk"
// MAGIC     .split(" ")
// MAGIC     .map((_, 1))
// MAGIC     .groupBy(_._1)
// MAGIC     .mapValues(v => v.map(_._2).reduce(_+_))
// MAGIC ```
// MAGIC

// COMMAND ----------

// ---TESTING AREA---
/*
"sheena is a punk rocker she is a punk punk"
    .split(" ")
    .map(s => (s, 1))
    .groupBy(p => p._1)
    //.mapValues(v => v.length)
*/
"sheena is a punk rocker she is a punk punk"
    .split(" ")
    .map((_, 1))
    .groupBy(_._1)
    .mapValues(v => v.map(_._2).reduce(_+_))

// COMMAND ----------

// MAGIC %md
// MAGIC `"sheena is a punk rocker she is a punk punk"
// MAGIC     .split(" ")
// MAGIC     .map(s => (s, 1))
// MAGIC     .groupBy(p => p._1)
// MAGIC     .mapValues(v => v.length)`
// MAGIC
// MAGIC 1. `.split(" ")` splits the string into an array of words.
// MAGIC 2. `.map(s => (s, 1))` transforms each word into a tuple with the word itself and the number 1.
// MAGIC 3. `.groupBy(p => p._1)` groups these tuples by the word, resulting in a map where each key is a unique word and its value is a list of tuples containing that word. Example: `punk -> Array((punk,1), (punk,1), (punk,1))`
// MAGIC 4. `.mapValues(v => v.length)` transforms each group's value (the list of tuples) into the length of that list, which is the count of how many times each word appears in the original string. Example: `punk -> 3`
// MAGIC
// MAGIC `"sheena is a punk rocker she is a punk punk"
// MAGIC     .split(" ")
// MAGIC     .map((_, 1))
// MAGIC     .groupBy(_._1)
// MAGIC     .mapValues(v => v.map(_._2).reduce(_+_))`
// MAGIC
// MAGIC Does basically the same thing as above.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 6 - Approximation for fifth root
// MAGIC
// MAGIC Write a function, `fifthRoot`, that returns an approximate value for the fifth root of the input. Use the Newton's method, [https://en.wikipedia.org/wiki/Newton's_method](https://en.wikipedia.org/wiki/Newton%27s_method), with the initial guess of 1. For the fifth root this Newton's method translates to:
// MAGIC
// MAGIC $$y_0 = 1$$
// MAGIC $$y_{n+1} = \frac{1}{5}\bigg(4y_n + \frac{x}{y_n^4}\bigg) $$
// MAGIC
// MAGIC where `x` is the input value and `y_n` is the guess for the cube root after `n` iterations.
// MAGIC
// MAGIC Example steps when `x=32`:
// MAGIC
// MAGIC $$y_0 = 1$$
// MAGIC $$y_1 = \frac{1}{5}\big(4*1 + \frac{32}{1^4}\big) = 7.2$$
// MAGIC
// MAGIC $$y_2 = \frac{1}{5}\big(4*7.2 + \frac{32}{7.2^4}\big) = 5.76238$$
// MAGIC
// MAGIC $$y_3 = \frac{1}{5}\big(4*5.76238 + \frac{32}{5.76238^4}\big) = 4.61571$$
// MAGIC
// MAGIC $$y_4 = \frac{1}{5}\big(4*4.61571 + \frac{32}{4.61571^4}\big) = 3.70667$$
// MAGIC
// MAGIC $$...$$
// MAGIC
// MAGIC You will have to decide yourself on what is the condition for stopping the iterations. (you can add parameters to the function if you think it is necessary)
// MAGIC
// MAGIC Note, if your code is running for hundreds or thousands of iterations, you are either doing something wrong or trying to calculate too precise values.

// COMMAND ----------

def fifthRoot(x: Double): Double = {
  var n = 0
  var yn = 1.0
  while (n < 90) {
    yn = 1.0/5.0*(4.0*yn + x/Math.pow(yn,4))
    n += 1
  }
  yn // Return the computed value
}

println(f"Fifth root of 32:       ${fifthRoot(32)}%.15f")
println(f"Fifth root of 3125:     ${fifthRoot(3125)}%.15f")
println(f"Fifth root of 10^10:    ${fifthRoot(1e10)}%.15f")
println(f"Fifth root of 10^(-10): ${fifthRoot(1e-10)}%.15f")
println(f"Fifth root of -243:     ${fifthRoot(-243)}%.15f")
println("==============================")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Example output
// MAGIC
// MAGIC (the exact values are not important, but the results should be close enough)
// MAGIC
// MAGIC ```text
// MAGIC Fifth root of 32:       2.0000000000000244
// MAGIC Fifth root of 3125:     5.000000000000007
// MAGIC Fifth root of 10^10:    100.00000005161067
// MAGIC Fifth root of 10^(-10): 0.010000000000000012
// MAGIC Fifth root of -243:     -3.0000000040240726
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 7 - First Spark task
// MAGIC
// MAGIC Create and display a DataFrame with your own data similarly as was done in the tutorial notebook.
// MAGIC
// MAGIC Then fetch the number of rows from the DataFrame.

// COMMAND ----------

import org.apache.spark.sql.DataFrame

val myData = Seq(
  ("Apple", 0.99),
  ("Banana", 0.59),
  ("Pineapple", 2.55)
)
val myDF: DataFrame = spark.createDataFrame(myData).toDF("Product", "Price")

myDF.show()

// COMMAND ----------

val numberOfRows: Long = myDF.count()

println(s"Number of rows in the DataFrame: ${numberOfRows}")
println("==============================")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Example output
// MAGIC (the actual data can be totally different):
// MAGIC
// MAGIC ```text
// MAGIC +----------------------+-------+------+
// MAGIC |                  Name|Founded|Titles|
// MAGIC +----------------------+-------+------+
// MAGIC |               Arsenal|   1886|    13|
// MAGIC |               Chelsea|   1905|     6|
// MAGIC |             Liverpool|   1892|    19|
// MAGIC |       Manchester City|   1880|     9|
// MAGIC |     Manchester United|   1878|    20|
// MAGIC |Tottenham Hotspur F.C.|   1882|     2|
// MAGIC +----------------------+-------+------+
// MAGIC Number of rows in the DataFrame: 6
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 8 - Second Spark task
// MAGIC
// MAGIC The CSV file `numbers.csv` contains some data on how to spell numbers in different languages. The file is located in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2024-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2024gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) in folder `exercises/ex1`.
// MAGIC
// MAGIC Load the data from the file into a DataFrame and display it.
// MAGIC
// MAGIC Also, calculate the number of rows in the DataFrame.

// COMMAND ----------

val numberDF: DataFrame = spark.read
                          .option("header", "true")
                          .option("sep", ",")
                          .option("inferSchema", "true")
                          .csv("abfss://shared@tunics320f2024gen2.dfs.core.windows.net/exercises/ex1/numbers.csv")

display(numberDF)

// COMMAND ----------

val numberOfNumbers: Long = numberDF.count()

println(s"Number of rows in the number DataFrame: ${numberOfNumbers}")
println("==============================")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Example output:
// MAGIC
// MAGIC ```text
// MAGIC +------+-------+---------+-------+------+
// MAGIC |number|English|  Finnish|Swedish|German|
// MAGIC +------+-------+---------+-------+------+
// MAGIC |     1|    one|     yksi|    ett|  eins|
// MAGIC |     2|    two|    kaksi|    twå|  zwei|
// MAGIC |     3|  three|    kolme|    tre|  drei|
// MAGIC |     4|   four|    neljä|   fyra|  vier|
// MAGIC |     5|   five|    viisi|    fem|  fünf|
// MAGIC |     6|    six|    kuusi|    sex| sechs|
// MAGIC |     7|  seven|seitsemän|    sju|sieben|
// MAGIC |     8|  eight|kahdeksan|   åtta|  acht|
// MAGIC |     9|   nine| yhdeksän|    nio|  neun|
// MAGIC |    10|    ten| kymmenen|    tio|  zehn|
// MAGIC +------+-------+---------+-------+------+
// MAGIC Number of rows in the number DataFrame: 10
// MAGIC ```
