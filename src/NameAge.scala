import scala.math.min

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark._

/** Find the minimum temperature by weather station */
object NameAge extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def parseLine(line: String) = {
    val fields = line.split(",")
    val name = fields(0)
    val age = fields(1)
    val state = fields(2)
    var flag = "N"

    if (fields(1).toInt > 18) {
      flag = "Y"
    }

    (name, age, state, flag)
  }

  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Create a SparkContext using every core of the local machine
  val sc = new SparkContext("local[*]", "NameAge")
  val input = sc.textFile(
    "/home/gaurav/Documents/BigData/week-9/datasets/name-age-city.csv"
  )

//   val rdd2 = input.map(line => {
//         val fields = line.split(",")
//         if (fields(1).toInt > 18)
//             (fields(0),fields(1),fields(2),"Y")
//         else
//             (fields(0),fields(1),fields(2),"N")
//         })

  val rdd2 = input.map(parseLine)

  rdd2.collect().foreach(println)
}
