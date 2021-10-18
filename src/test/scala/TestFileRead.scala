import spray.json.DefaultJsonProtocol._
import spray.json._
// if you don't supply your own Protocol (see below)

/**
  * Created by guruprasad.gv on 8/26/17.
  */
object TestFileRead {


  def main(args: Array[String]): Unit = {
    val t = scala.io.Source.fromFile("/Users/guruprasad.gv/Documents/WorkDocs/ex-ss").mkString
    val json = t.parseJson

    println(json.asJsObject.fields.get("inputs").get.convertTo[JsArray].elements(0).asJsObject.fields.get("t1").get.convertTo[String])

  }
}
