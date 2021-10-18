import com.github.nscala_time.time.Imports._
import org.joda.time.DateTime

/**
  * Created by guruprasad.gv on 8/27/17.
  */
object DateO {

  def main(args: Array[String]): Unit = {
    val n = DateTime.now();
    val universe: scala.reflect.runtime.universe.type = scala.reflect.runtime.universe
    import universe._



    println("::" + Duration.parse("3.day").getMillis); // (richReadableDuration("3.days").toDateTime));
    println(q"3 + 4")
  }
}
