import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object CountLogMessage extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "CountLogMessage")
  println("test")
  val logList = List ("ERROR: Thu Jun 04 10:37:51 BST 2015",
  "WARN: Sun Nov 06 10:37:51 GMT 2016",
  "WARN: Mon Aug 29 10:37:51 BST 2016",
  "ERROR: Thu Dec 10 10:37:51 GMT 2015",
  "ERROR: Fri Dec 26 10:37:51 GMT 2014",
  "ERROR: Thu Feb 02 10:37:51 GMT 2017",
  "WARN: Fri Oct 17 10:37:51 BST 2014",
  "ERROR: Wed Jul 01 10:37:51 BST 2015",
  "WARN: Thu Jul 27 10:37:51 BST 2017",
  "WARN: Thu Oct 19 10:37:51 BST 2017",
  "WARN: Wed Jul 30 10:37:51 BST 2014")

  val logListRdd = sc.parallelize(logList)
  val pairRdd = logListRdd.map(x => {
    val columns = x.split(":")(0)
    (columns,1)
  })
  val resultantRdd = pairRdd.reduceByKey((x,y) => (x+y))

  resultantRdd.collect().foreach(println)
}
