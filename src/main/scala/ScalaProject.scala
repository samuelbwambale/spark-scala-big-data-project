import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, variance}
import org.apache.spark.{SparkConf, SparkContext}

case class LabourIncomes(id: String, wages: String, education: String, age: String, sex: String, language: String)

object ScalaProject extends App{

  override def  main(args: Array[String]): Unit = {

    val category = if (args.length > 0 && args(0) != null) args(0) else "sex"; // Category Variable
    val numeric = if (args.length > 0 && args(1) != null) args(1) else "wages"; // Numeric Variable
    val samples: Int = if (args.length > 0 && args(2) != null) args(2).asInstanceOf[Int] else 10;

    val sparkConfig = new SparkConf().setAppName("Labour And Income Dynamics").setMaster("local")
    val sc = new SparkContext(sparkConfig)

    sc.setLogLevel("WARN")

    // Initialize an SQLContext
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    //loading csv data of labour and income dynamics
    val dataCsv = sc.textFile("SLID.csv")

    // Create a Spark DataFrame
    val headerAndRows = dataCsv.map(line => line.split(",").map(_.trim))
    val header = headerAndRows.first
    val data = headerAndRows.filter(_(0) != header(0))
    val labourData = data
      .map(p => LabourIncomes(p(0), p(1), p(2), p(3), p(4), p(5)))
      .toDF
        labourData.printSchema
//        labourData.select("wages").show(5)
//        labourData.filter(labourData("wages") < 18).show()

    // output the average and variance of the overall labour dynamics
    val labourIncomeMetrics = labourData
      .groupBy(category)
      .agg(avg(numeric).as("Average Wage"), variance(numeric).as("Wage Variance"))

    // take initial sample from whole labour income dynamics with no replacement
    val sample = labourData.sample(false, .25)

    // compute average and variance metrics for sample taken
    val average = sample.groupBy(category).agg(avg(numeric).as("Average Wage"), variance(numeric).as("Wage Variance")).toDF
    val variances = sample.groupBy(category).agg(variance(numeric).as("Wage Variance")).toDF

    // add average and variance of initial sample to the collections of the corresponding metrics
    var avgColl: List[DataFrame] = List(average)
    var varColl: List[DataFrame] = List(variances)

    // resample with replacement, compute average & variance metrics for each sample, and append them to corresponding collections
    for (i <- 1 until (samples - 1)) {
      val resample = sample.sample(true, 1)
      avgColl ::= resample.groupBy(category).agg(avg(numeric), variance(numeric)).toDF
    }

    // reduce respective collections to get average & variance across n samples
    val flatAvg = avgColl
      .flatMap(df => df.flatMap(row => Map(row.getString(0) -> row.getDouble(1))).collect().toList)
    val sampAvg = sc.parallelize(flatAvg)
      .reduceByKey(_ + _)
      .map(x => x._1 -> x._2 / samples)
      .toDF

    val flatVar = avgColl
      .flatMap(df => df.map(row => row.getString(0) -> row.getDouble(2)).collect().toList)
    val sampVar = sc.parallelize(flatVar)
      .reduceByKey(_ + _)
      .map(x => x._1 -> x._2 / samples)
      .toDF()

    // Output away...
    labourIncomeMetrics.show()
    println("Average Wage after sampling")
    sampAvg.show()
    println("Wage Variance after sampling")
    sampVar.show()
  }

}
