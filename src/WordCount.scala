import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger



object WordCount extends App {
  val sc = new SparkContext("local[*]", "wordcount")
  Logger.getLogger("org").setLevel(Level.ERROR)
  val input = sc.textFile("/home/gaurav/Documents/BigData/week-9/datasets/search_data-201008-180523.txt")
  val words = input.flatMap(x => x.split(" "))
//  val wordMap = words.map(x => (x,1))
  val wordMap = words.map((_,1))
  val finalCount = wordMap.reduceByKey((a,b) => a + b)
  finalCount.collect().foreach(println)

  scala.io.StdIn.readLine()

}