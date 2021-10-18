import com.tfs.dp.spartan.Spartan
import com.tfs.dp.spartan.manager.TableManager

/**
  * Created by Srinidhi.BS on 10/13/17.
  */
object RawAssistTranscriptTest {

  def main(args: Array[String]): Unit = {

    val spartan: Spartan = new Spartan("/home/user/Desktop/data")

    val table: TableManager = spartan.use("RawAssistTranscripts", "nemo-client-cap1enterprise", "201709102300", "201709102300")

    val df = spartan.spark.sql("select * from RawAssistTranscripts")

    df.printSchema()
    df.show(10, false)
  }
}
