
/**************************************************
 * Note: In Maven dependency                      *
 * Spark and Spark SQL must have the same version *
 * This code is created using 2.11 version 1.6.0  *
 ***************************************************/
import java.io._
import scala.io.Source
import scala.math.random
import org.apache.spark._
import org.apache.spark.sql.{DataFrame, SQLContext}
import au.com.bytecode.opencsv.CSVParser

case class Person(name: String, age: Int)

case class Cars(car: String, mpg: String, cyl: String, disp: String, hp: String,
                drat: String,wt: String, qsec: String, vs: String, am: String, gear: String, carb: String)


object SparkDataFrame extends App{
  override def  main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark and SparkSql").setMaster("local")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    // Exploring SparkSQL
    // Initialize an SQLContext
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext._
    import sqlContext.implicits._

    // Load a cvs file
    val csv = sc.textFile("mtcars.csv")
    // Create a Spark DataFrame
    val headerAndRows = csv.map(line => line.split(",").map(_.trim))
    val header = headerAndRows.first
    val mtcdata = headerAndRows.filter(_(0) != header(0))
    val mtcars = mtcdata
      .map(p => Cars(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11)))
      .toDF
    mtcars.printSchema
    mtcars.select("mpg").show(5)
    mtcars.filter(mtcars("mpg") < 18).show()
    // Aggregate data after grouping by columns
    import org.apache.spark.sql.functions._
    mtcars.groupBy("cyl").agg(avg("wt")).show()
    mtcars.groupBy("cyl").agg(count("wt")).sort($"count(wt)".desc).show()
    // Operate on columns
    mtcars.withColumn("wtTon", mtcars("wt") * 0.45).select(("car"),("wt"),("wtTon")).show(6)
    // Run SQL queries from the Spark DataFrame
    mtcars.registerTempTable("cars")
    val highgearcars = sqlContext.sql("SELECT car, gear FROM cars WHERE gear >= 5")
    highgearcars.show()

    // DataFrame from Scala objects
    val info = List(("mike", 24), ("joe", 34), ("jack", 55))
    val infoRDD = sc.parallelize(info)
    val people = infoRDD.map(r => Person(r._1, r._2)).toDF()
    people.registerTempTable("people")
    val subDF = sqlContext.sql("select * from people where age > 30")
    subDF.show()

    var myOne = List(
      List("cat", "mat","bat"),List("hat","mat","rat"),List("cat","mat","sat"),List("cat","fat","bat"))
    myOne.foreach(println)
    println()
    var myTwo = sc.parallelize(myOne)
    myTwo.foreach(println)
    println()
    var myThree = myTwo.map(x => (x(1), x(2)))
    myThree.foreach(println)
    println()
    var myFour = myThree.sortBy(_._1, false).sortBy(_._2)
    myFour.foreach(println)
    println()
    val data1 = Array(7, 8, 2, 10, 4, 10, 9, 4)
    val rdd1 = sc.parallelize(data1)
    val max1 = rdd1.max()
    println("max1 is " + max1)
    val rdd2 = rdd1.filter(_ != max1)
    val max2 = rdd2.max()
    println("max2 is " + max2)

    println()
    val rdd3 = sc.parallelize(List(("Mech", 30), ("Mech", 40), ("Elec", 50)))
      val valmapped = rdd3.mapValues(mark => (mark, 1));
      val reduced
      = valmapped.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      println(reduced.collect().mkString(", "))



  }
}


