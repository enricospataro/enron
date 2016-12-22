
/**
  * Created by espataro on 11/12/16.
  */
import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.file.{Path, Paths}
import java.util.zip._

import org.apache.hadoop.hdfs.util.Diff.ListType
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class Test extends Serializable {

  val path = "/Users/espataro/Documents/enron/edrm-enron-v2"
  val pathToExtract = "/Users/espataro/Documents/enron/enron_extract"
  val pathToTextFiles = "/Users/espataro/Documents/enron/enron_extract/text_000"

  val sc = new SparkContext("local","EnronXML")
  val sqlContext = SQLContextSingleton.getInstance(sc)

  def computeZips(pathToZipFolder: Path) = {
    unzip(Paths.get(pathToExtract))

    val xml = sqlContext
        .read
        .format("com.databricks.spark.xml")
        .option("rootTag", "books")
        .option("rowTag", "Document")
        .load("/Users/espataro/Documents/enron/enron_extract/*.xml")

    val top_recipients = computeTopRecipients(xml)
    val avgLength = computeAverageLength(xml)
  }

  private def computeTopRecipients(xml: DataFrame): List[String]= {
    import sqlContext.implicits._

    val df = xml.select("Tags.Tag")

    val recipients_df = df
      .select(explode(col("Tag")).as("collection"))
      .select(col("collection.*"))
      .select("_TagName", "_TagValue")
      .where("_TagName = '#To' or _TagName = '#CC'")

    val recipients_df_parsed = recipients_df.map {
      case Row(name: String, value: String) => Row(name, emailParser(value))
    }

    val recipients_df2_exploded = sqlContext
      .createDataFrame(recipients_df_parsed, recipients_df.schema)
      .withColumn("_TagValue", explode(split($"_TagValue", ",")))

    recipients_df2_exploded
      .withColumn("_TagWeight", when(recipients_df2_exploded("_TagName") === "#To", 1.0)
      .otherwise(0.5))
      .drop("_TagName")
      .registerTempTable("df")

    sqlContext.sql("select _TagValue, sum(_TagWeight) from df group by _TagValue")
      .orderBy(desc("_c1"))
      .drop("_c1")
      .takeAsList(100)
  }

  private def computeAverageLength(xml: DataFrame): Double = {
    import sqlContext.implicits._

    xml.select("_DocID", "_DocType").where("_DocType = 'Message'").registerTempTable("message_df")

    sqlContext.sql("select _DocID from message_df")
      .map {
        case Row(file: String) => countWords(file)
      }
      .toDF()
      .select(avg($"_1"))
      .first()
      .getDouble(0)
  }

  private def countWords(fileName: String): Int = {

    val pathToFile = pathToTextFiles+"/"+ fileName + ".txt"
    val src = scala.io.Source.fromFile(pathToFile)
    val count =
      (for {
        line <- src.getLines
      } yield {
        val words = line.split(" ")
        words.size
      }).sum
    count
    }

  private def emailParser(address: String): String = {
    val emails = """[\w\.-]+@[\w\.-]+""".r.findAllIn(address).toList
    val enrons = """CN=\w+>""".r.findAllIn(address).toList.map(x => x.substring(3, x.length - 1))

    (emails ++ enrons).filterNot(_.contains(" ")).mkString(",")

  }

  private def unzip(destination: Path): Unit = {

    val dirFiles = new File(path).listFiles()

    dirFiles.foreach { file =>
      val zis = new ZipInputStream(new FileInputStream(file))

      Stream.continually(zis.getNextEntry).takeWhile(_ != null).foreach { file =>
        if (!file.isDirectory) {
          val outPath = destination.resolve(file.getName)
          val outPathParent = outPath.getParent
          if (!outPathParent.toFile.exists())
            outPathParent.toFile.mkdirs()

          val outFile = outPath.toFile
          val out = new FileOutputStream(outFile)
          val buffer = new Array[Byte](4096)
          Stream.continually(zis.read(buffer)).takeWhile(_ != -1).foreach(out.write(buffer, 0, _))
        }
      }
    }
  }
    }

  object SQLContextSingleton {

    @transient private var instance: SQLContext = _

    def getInstance(sparkContext: SparkContext): SQLContext = {
      if (instance == null) {
        instance = new SQLContext(sparkContext)
      }
      instance
    }
  }

  object test {
    def main(args: Array[String]): Unit = {
      val path = "/Users/espataro/Documents/enron/edrm-enron-v2/"
      new Test().computeZips(Paths.get(path))
    }
  }
