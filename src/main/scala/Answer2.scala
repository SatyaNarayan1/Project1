import org.apache.hadoop.mapred.FileSplit
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

object Answer2 {

  def main(args:Array[String]) {
    val sc = new SparkContext("local[4]", "Product with highest transaction amount")
    val sqlContext = new SQLContext(sc)

    val mailRDD = sc.textFile("src/main/resources/mail_sample*")
    val lineRDD = mailRDD.mapPartitionsWithSplit {
      (inputSplit, iterator) =>{
        var from = ""
        var to = ""
        var sent = ""
        var cc = ""
        var subject = ""
        var body =""
       val a =  iterator.map{tpl => {
          if(tpl.startsWith("From:"))
          from = tpl.split(":")(1)
          else if(tpl.startsWith("To:"))
            to = tpl.split(":")(1)
          else if(tpl.startsWith("Sent:"))
            sent = tpl.split(":")(1)
          else if(tpl.startsWith("Cc:"))
            cc = tpl.split(":")(1)
          else if(tpl.startsWith("Subject:"))
            subject = tpl.split(":").drop(1).mkString(":")
          else
          body = body+"\\n"+ tpl
          Row(from,to,sent,cc,subject,body)
        }}.toArray
        a.slice(a.size-1 ,a.size).iterator
      }
    }

    lineRDD.foreach(println)

    val mailSchema = StructType(List(
      StructField("From",StringType,false),
      StructField("To",StringType,false)
      ,StructField("Sent",StringType,false)
      ,StructField("CC",StringType,false)
      ,StructField("Subject",StringType,false)
      ,StructField("Body",StringType,false)
    ))
    val mailDF = sqlContext.createDataFrame(lineRDD,mailSchema)
    mailDF.show(true)


  }
}
