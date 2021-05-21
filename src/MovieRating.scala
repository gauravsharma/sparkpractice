import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object  MovieRating extends App {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "movierating")
    val input = sc.textFile("/home/gaurav/Documents/BigData/week-9/datasets/moviedata-201008-180523.data")

    val ratings = input.map(x => (x.split("\t")(1), x.split("\t")(2)))

    val ratingsMap = ratings.map(x => (x,1))
    val ratingResult = ratingsMap.reduceByKey((x,y) => x+y)

    val result = ratingResult.collect
    result.foreach(println)

    // scala.io.StdIn.readLine()
}