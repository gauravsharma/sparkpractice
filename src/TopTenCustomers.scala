import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object  TopTenCustomers extends App {
    // Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "toptencustomers")
    val input = sc.textFile("/home/gaurav/Documents/BigData/week-9/datasets/customerorders-201008-180523.csv")

    val processedInput = input.map(x => (x.split(",")(0), x.split(",")(2).toFloat))

    val totalByCustomer = processedInput.reduceByKey((x,y) => x+y)
    val premiumCustomer = totalByCustomer.filter(x => x._2 > 5000)
    val doubledAmount = premiumCustomer.map(x => (x._1, x._2*2)).cache()
    // val sortedResults = totalByCustomer.sortBy(x => x._2, false)

    val result = doubledAmount.collect
    result.foreach(println)

    println(doubledAmount.count)

    scala.io.StdIn.readLine()
}