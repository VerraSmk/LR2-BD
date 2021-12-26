import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel


object Main {

  def parseLanguage(row: String): Array[String] = {
    val words = row.split(",")
    Array(words(0).toLowerCase)
  }

  def getDateAndTag(e: scala.xml.Elem) = {
    val creationDate = e.attribute("CreationDate")
    val tags = e.attribute("Tags")
    (creationDate, tags)
  }

  def parseDateAndTags(e: (String, String)) = {
    val year = e._1.substring(0, 4)
    val tagsAr = e._2.substring(4, e._2.length - 4).split("&gt;&lt;")
    (year, tagsAr)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)
    val cfg = new SparkConf().setAppName("Test").setMaster("local[2]")
    val hadoopRoot = "hdfs://localhost:9000/root/"
    val sc = new SparkContext(cfg)

    val spark = SparkSession
      .builder()
      .getOrCreate()

    import spark.implicits._

    val years = (2010 to 2021).map(x => x.toString)

    val topCount = 10

    val startDate = LocalDateTime.now()

    val programmingLanguagesFile = sc.textFile(hadoopRoot + "programming-languages.csv")

    val header = programmingLanguagesFile.first()

    val programmingLanguagesRDD = programmingLanguagesFile
      .filter(row => row != header)
      .flatMap(parseLanguage)
      .collect()


    //val postsRDD = sc.textFile("K://BIGData/Posts.xml", 100)
    val postsRDD = sc.textFile(hadoopRoot+"posts_sample.xml")
    val posts_count = postsRDD.count
    val posts_raw = postsRDD.zipWithIndex.filter { case (s, idx) => idx > 2 && idx < posts_count - 1 }.map(_._1)
    val posts_xml = posts_raw.map(row => scala.xml.XML.loadString(row))


    val postYearTags = posts_xml.map(getDateAndTag).filter {
      x => x._1.isDefined && x._2.isDefined
    }.map {
      x => (x._1.mkString, x._2.mkString)
    }.map(parseDateAndTags)

    val yearTags = postYearTags.flatMap {
      case (year, tags) => tags.map(tag => (year, tag))
    }.filter { case (year, tag) => programmingLanguagesRDD.contains(tag) }
      .cache()

    val yearsTagCounts = years.map { reportYear =>
      yearTags.filter {
        case (tagYear, tag) => reportYear == tagYear
      }.map {
        case (tagYear, tag) => (tag, 1)
      }.reduceByKey {
        (a, b) => a + b
      }.map { case (tag, count) =>
        (reportYear, tag, count)
      }
    }


    val topYearsTagCounts = yearsTagCounts.map { yearTagsCounts =>
      yearTagsCounts.sortBy { case (year, tag, count) => -count }.take(topCount)
    }

    val finalReport = topYearsTagCounts.reduce((a, b) => a.union(b))

    val finalDataFrame = sc.parallelize(finalReport).toDF("Year", "Language", "Count")

    spark.createDataset(finalReport).repartition(1).write.parquet("report")

    val stopTime = LocalDateTime.now()

    finalDataFrame.show(years.size * topCount)
    println(s"duration ${java.time.Duration.between(startDate, stopTime)}")
  }
}

