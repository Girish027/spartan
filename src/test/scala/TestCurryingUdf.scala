/**
  * Created by guruprasad.gv on 8/29/17.
  */

import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

object TestCurryingUdf {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("a").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc);
    val spark = sqlContext.sparkSession
    import spark.implicits._
    // Just create a simple dataframe with integers from 0 to 999.
    val rowRDD = sc.parallelize(0 to 999).map(Row(_))
    val schema = StructType(StructField("value", IntegerType, true) :: Nil)
    val rowDF = spark.createDataFrame(rowRDD, schema)

    // Here we use currying: hideTabooValues is a partial function of type (List[Int]) => UserDefinedFunction
    def hideTabooValues(taboo: List[Int]) = udf((n: Int) => if (taboo.contains(n)) -1 else n)

    // Semplifying, you can see hideTabooValues as a UDF factory, that specialises the given UDF definition at invocation time.
    // This will show that, without giving a parameter, hideTabooValues is just a function.
    hideTabooValues _
    // res7: List[Int] => org.apache.spark.sql.UserDefinedFunction = <function1>

    // It's time to try our UDF! Let's define the taboo list
    val forbiddenValues = List(0, 1, 2)

    // And then use Spark SQL to apply the UDF. You can see two invocation here: the first creates the specific UDF
    // with the given taboo list, and the second uses the UDF itself in a classic select instruction.
    rowDF.filter(hideTabooValues(forbiddenValues)(rowDF("value")) === $"value").show(6)

  }

}
