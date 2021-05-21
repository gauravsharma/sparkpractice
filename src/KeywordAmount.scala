import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object KeywordAmount {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "wordcount")
  val initial_rdd = sc.textFile("/home/gaurav/Documents/BigData/week-10/bigdatacampaigndata-201014-183159.csv")
  val mappedInput = initial_rdd.map(x => (x.split(",")(10).toFloat,x.split(",")(0)))
  val words = mappedInput.flatMapValues(x => x.split(" "))
  val finalMapped = words.map(x =>(x._2.toLowerCase(),x._1))
  val total = finalMapped.reduceByKey((x,y) => x+y)
  val sorted = total.sortBy(x => x._2, false)
  sorted.take(20).foreach(println)
}