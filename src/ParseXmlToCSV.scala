import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import scala.xml._

object ParseXmlToCSV extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first application")
  sparkConf.set("spark.master", "local[2]")
  
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  
  
  val df = spark.read.format("xml")
  .option("rowTag", "data")
  .option("path", "/home/gaurav/Documents/BigData/datasets/API_NE.CON.PRVT.CN.AD_DS2_en_xml_v2_2259493.xml")
  .load
  
//  df.createOrReplaceTempView("tempname")
  
  df.collect.foreach(println)
}