import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StructType,StructField,IntegerType,StringType}
import org.apache.spark.sql.{SQLContext,Row,DataFrame}
import org.apache.spark.sql.functions._




object Answer1 {

  def main(args:Array[String]){
  val sc = new SparkContext("local[4]", "Product with highest transaction amount")
  val sqlContext = new SQLContext(sc)

    val salesDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/Input_file_for_Hadoop_screening.csv")

    salesDF.printSchema()
    salesDF.registerTempTable("salesDF")
//1st Answer
    val topTxProduct = sqlContext.sql("select Product,max(Transaction_amount) from salesDF group by Product order by 2 desc").take(1).foreach(println)

//2nd Answer
    sqlContext.sql("select Country,count(1) from salesDF where Payment_Type='Mastercard' group by Country order by 2 desc").take(1).foreach(println)
//3rd Answer
    sqlContext.sql("select date_format(TO_DATE(CAST(UNIX_TIMESTAMP(Transaction_date, 'MM/dd/yy HH:mm') AS TIMESTAMP)),'MM/yyyy') month ,sum(Transaction_amount) total_transaction_amount,count(1) as no_of_transaction from salesDF group by date_format(TO_DATE(CAST(UNIX_TIMESTAMP(Transaction_date, 'MM/dd/yy HH:mm') AS TIMESTAMP)),'MM/yyyy')  ").show
    //4th Answer
    sqlContext.sql("select Payment_Type,count(distinct Name) as Users from salesDF group by Payment_Type").show
  }
}
