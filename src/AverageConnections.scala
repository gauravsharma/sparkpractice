import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object  AverageConnections extends App {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "averageconnections")
    val input = sc.textFile("/home/gaurav/Documents/BigData/week-9/datasets/friendsdata-201008-180523.csv")

    val mappedInput = input.map(x => (x.split("::")(2).toInt, x.split("::")(3).toInt)) //returns (34, 300) (34,100) (34,200)
    val ageMappedInput = mappedInput.mapValues(x => (x,1))

    val sumAgeWise = ageMappedInput.reduceByKey((x,y) => (x._1+y._1, x._2+y._2)) //returns (34,(1473,6))
    val mappedAgeWise = sumAgeWise.mapValues(x => (x._1/x._2)).sortBy(x => x._2, false) //if keys are same then we can use mapValues which directly works on value part.

    val result = mappedAgeWise.collect
    result.foreach(println)

    // scala.io.StdIn.readLine()
}