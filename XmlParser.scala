import scala.xml.XML

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.hadoop.streaming.StreamInputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.io.Text
import org.apache.log4j.Logger
import org.apache.log4j.Level

Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)

val pathToFiles = "/user/cloudera/test1.xml"
val outputData = "/user/cloudera/testop"

val jobConf = new JobConf()
    jobConf.set("stream.recordreader.class","org.apache.hadoop.streaming.StreamXmlRecordReader")
    jobConf.set("stream.recordreader.begin","<book ")
    jobConf.set("stream.recordreader.end","</book>")
    FileInputFormat.addInputPaths(jobConf,pathToFiles)

val xmlData = sc.hadoopRDD(
        jobConf,
        classOf[org.apache.hadoop.streaming.StreamInputFormat],
        classOf[Text],
        classOf[Text])
    
   
val xml =xmlData.map(_._1.toString())
 
val aaa=xml.map{ x => 
      val ttt = XML.loadString(x);
	  
	  val myData =ttt.map{x =>
      val book_id = (x \ "@id").text
      val author = (x \"author").text
      val title = (x \"title").text
      val genre = (x\ "genre").text
      val price = (x \"price").text
      val pub_date = (x\"publish_date").text
      val description = (x\"description").text
      val row=book_id+","+author+"," +title+","+genre+","+price+","+pub_date+","+description
      (row)
    }
   (myData)
}

