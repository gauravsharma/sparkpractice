import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import java.sql.Timestamp
import org.apache.spark.sql.Dataset

case class OrdersData(order_id: Int, order_date: Timestamp, order_customer_id: Int, order_status:String)

object DataFranesExample extends App {
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first application")
  sparkConf.set("spark.master", "local[2]")
  
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  
  val ordersSchemaDDL = "orderid Int, orderdate String, custid Int, ordstatus String"
  
  val ordersDf = spark.read
  .format("csv")
  .option("header", true)
  .schema(ordersSchemaDDL)
//  .option("inferSchema", true)
  .option("path", "/home/gaurav/Documents/BigData/week-12/datasets/orders-201025-223502.csv")
  .load  
//  val groupedOrdersDf = ordersDf.repartition(4)
//  .where("order_customer_id > 10000")
//  .select("order_id", "order_customer_id")
//  .groupBy("order_customer_id")
//  .count()
  
//  import spark.implicits._
//  val ordersDs = ordersDf.as[OrdersData]
  
//  ordersDs.filter(x => x.order_id < 10)
  
//  ordersDf.filter("order_id < 10")
  
//  groupedOrdersDf.show()
  
  scala.io.StdIn.readLine()
  
  spark.stop()
}