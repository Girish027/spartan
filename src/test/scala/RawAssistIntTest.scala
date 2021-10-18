import com.tfs.dp.spartan.Spartan
import com.tfs.dp.spartan.manager.TableManager

/**
  * Created by guruprasad.gv on 10/13/17.
  */
object RawAssistIntTest {

  def main(args: Array[String]): Unit = {

    val spartan: Spartan = new Spartan("/home/user/Desktop/data")

    val table: TableManager = spartan.use("RawAssistInteractions", "nemo-client-cap1enterprise", "201707010000", "201707010000")

    val df = spartan.spark.sql("select context.data.* from RawAssistInteractions")

    df.printSchema()
    df.show(10, false)
  }
}
