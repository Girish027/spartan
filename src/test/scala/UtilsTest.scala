
import com.tfs.dp.spartan.Utils
import com.tfs.dp.spartan.Utils.Granularity
import org.scalatest._


/**
  * Created by Srinidhi.bs on 11/26/17.
  */
class UtilsTest extends FlatSpec {


  "Test" should "verify path genearation for hourly granularity" in {

    val rootPath = "/rootPath"
    val startDate = "201710100000"
    val endDate = "201710110000"

    val finalPath = Utils.getFinalPathsAsString(rootPath, startDate, endDate, Granularity.HOUR)
    val expectedFinalPath = "/rootPath/year=2017/month=10/day=10/hour=00/min=*,/rootPath/year=2017/month=10/day=10/hour=01/min=*,/rootPath/year=2017/month=10/day=10/hour=02/min=*,/rootPath/year=2017/month=10/day=10/hour=03/min=*,/rootPath/year=2017/month=10/day=10/hour=04/min=*,/rootPath/year=2017/month=10/day=10/hour=05/min=*,/rootPath/year=2017/month=10/day=10/hour=06/min=*,/rootPath/year=2017/month=10/day=10/hour=07/min=*,/rootPath/year=2017/month=10/day=10/hour=08/min=*,/rootPath/year=2017/month=10/day=10/hour=09/min=*,/rootPath/year=2017/month=10/day=10/hour=10/min=*,/rootPath/year=2017/month=10/day=10/hour=11/min=*,/rootPath/year=2017/month=10/day=10/hour=12/min=*,/rootPath/year=2017/month=10/day=10/hour=13/min=*,/rootPath/year=2017/month=10/day=10/hour=14/min=*,/rootPath/year=2017/month=10/day=10/hour=15/min=*,/rootPath/year=2017/month=10/day=10/hour=16/min=*,/rootPath/year=2017/month=10/day=10/hour=17/min=*,/rootPath/year=2017/month=10/day=10/hour=18/min=*,/rootPath/year=2017/month=10/day=10/hour=19/min=*,/rootPath/year=2017/month=10/day=10/hour=20/min=*,/rootPath/year=2017/month=10/day=10/hour=21/min=*,/rootPath/year=2017/month=10/day=10/hour=22/min=*,/rootPath/year=2017/month=10/day=10/hour=23/min=*"
    assert(finalPath === expectedFinalPath)


  }

  "Test" should "verify sequence of paths genearation for hourly granularity" in {

    val rootPath = "/rootPath"
    val startDate = "201710100000"
    val endDate = "201710110000"

    val finalPath = Utils.getFinalPathAsList(rootPath, startDate, endDate, Granularity.HOUR, Some(""))
    val expectedFinalPath = Seq("/rootPath/year=2017/month=10/day=10/hour=00/min=*","/rootPath/year=2017/month=10/day=10/hour=01/min=*","/rootPath/year=2017/month=10/day=10/hour=02/min=*","/rootPath/year=2017/month=10/day=10/hour=03/min=*","/rootPath/year=2017/month=10/day=10/hour=04/min=*","/rootPath/year=2017/month=10/day=10/hour=05/min=*","/rootPath/year=2017/month=10/day=10/hour=06/min=*","/rootPath/year=2017/month=10/day=10/hour=07/min=*","/rootPath/year=2017/month=10/day=10/hour=08/min=*","/rootPath/year=2017/month=10/day=10/hour=09/min=*","/rootPath/year=2017/month=10/day=10/hour=10/min=*","/rootPath/year=2017/month=10/day=10/hour=11/min=*","/rootPath/year=2017/month=10/day=10/hour=12/min=*","/rootPath/year=2017/month=10/day=10/hour=13/min=*","/rootPath/year=2017/month=10/day=10/hour=14/min=*","/rootPath/year=2017/month=10/day=10/hour=15/min=*","/rootPath/year=2017/month=10/day=10/hour=16/min=*","/rootPath/year=2017/month=10/day=10/hour=17/min=*","/rootPath/year=2017/month=10/day=10/hour=18/min=*","/rootPath/year=2017/month=10/day=10/hour=19/min=*","/rootPath/year=2017/month=10/day=10/hour=20/min=*","/rootPath/year=2017/month=10/day=10/hour=21/min=*","/rootPath/year=2017/month=10/day=10/hour=22/min=*","/rootPath/year=2017/month=10/day=10/hour=23/min=*")
    assert(finalPath.equals(expectedFinalPath))


  }

  it should "verify path genearation for 15 min granularity" in {

    val rootPath = "/rootPath"
    val startDate = "201710100030"
    val endDate = "201710110015"

    val finalPath = Utils.getFinalPathsAsString(rootPath, startDate, endDate, Granularity.FIFTEEN_MIN)
    val expectedFinalPath = "/rootPath/year=2017/month=10/day=10/hour=00/min=30,/rootPath/year=2017/month=10/day=10/hour=00/min=45,/rootPath/year=2017/month=10/day=10/hour=01/min=00,/rootPath/year=2017/month=10/day=10/hour=01/min=15,/rootPath/year=2017/month=10/day=10/hour=01/min=30,/rootPath/year=2017/month=10/day=10/hour=01/min=45,/rootPath/year=2017/month=10/day=10/hour=02/min=00,/rootPath/year=2017/month=10/day=10/hour=02/min=15,/rootPath/year=2017/month=10/day=10/hour=02/min=30,/rootPath/year=2017/month=10/day=10/hour=02/min=45,/rootPath/year=2017/month=10/day=10/hour=03/min=00,/rootPath/year=2017/month=10/day=10/hour=03/min=15,/rootPath/year=2017/month=10/day=10/hour=03/min=30,/rootPath/year=2017/month=10/day=10/hour=03/min=45,/rootPath/year=2017/month=10/day=10/hour=04/min=00,/rootPath/year=2017/month=10/day=10/hour=04/min=15,/rootPath/year=2017/month=10/day=10/hour=04/min=30,/rootPath/year=2017/month=10/day=10/hour=04/min=45,/rootPath/year=2017/month=10/day=10/hour=05/min=00,/rootPath/year=2017/month=10/day=10/hour=05/min=15,/rootPath/year=2017/month=10/day=10/hour=05/min=30,/rootPath/year=2017/month=10/day=10/hour=05/min=45,/rootPath/year=2017/month=10/day=10/hour=06/min=00,/rootPath/year=2017/month=10/day=10/hour=06/min=15,/rootPath/year=2017/month=10/day=10/hour=06/min=30,/rootPath/year=2017/month=10/day=10/hour=06/min=45,/rootPath/year=2017/month=10/day=10/hour=07/min=00,/rootPath/year=2017/month=10/day=10/hour=07/min=15,/rootPath/year=2017/month=10/day=10/hour=07/min=30,/rootPath/year=2017/month=10/day=10/hour=07/min=45,/rootPath/year=2017/month=10/day=10/hour=08/min=00,/rootPath/year=2017/month=10/day=10/hour=08/min=15,/rootPath/year=2017/month=10/day=10/hour=08/min=30,/rootPath/year=2017/month=10/day=10/hour=08/min=45,/rootPath/year=2017/month=10/day=10/hour=09/min=00,/rootPath/year=2017/month=10/day=10/hour=09/min=15,/rootPath/year=2017/month=10/day=10/hour=09/min=30,/rootPath/year=2017/month=10/day=10/hour=09/min=45,/rootPath/year=2017/month=10/day=10/hour=10/min=00,/rootPath/year=2017/month=10/day=10/hour=10/min=15,/rootPath/year=2017/month=10/day=10/hour=10/min=30,/rootPath/year=2017/month=10/day=10/hour=10/min=45,/rootPath/year=2017/month=10/day=10/hour=11/min=00,/rootPath/year=2017/month=10/day=10/hour=11/min=15,/rootPath/year=2017/month=10/day=10/hour=11/min=30,/rootPath/year=2017/month=10/day=10/hour=11/min=45,/rootPath/year=2017/month=10/day=10/hour=12/min=00,/rootPath/year=2017/month=10/day=10/hour=12/min=15,/rootPath/year=2017/month=10/day=10/hour=12/min=30,/rootPath/year=2017/month=10/day=10/hour=12/min=45,/rootPath/year=2017/month=10/day=10/hour=13/min=00,/rootPath/year=2017/month=10/day=10/hour=13/min=15,/rootPath/year=2017/month=10/day=10/hour=13/min=30,/rootPath/year=2017/month=10/day=10/hour=13/min=45,/rootPath/year=2017/month=10/day=10/hour=14/min=00,/rootPath/year=2017/month=10/day=10/hour=14/min=15,/rootPath/year=2017/month=10/day=10/hour=14/min=30,/rootPath/year=2017/month=10/day=10/hour=14/min=45,/rootPath/year=2017/month=10/day=10/hour=15/min=00,/rootPath/year=2017/month=10/day=10/hour=15/min=15,/rootPath/year=2017/month=10/day=10/hour=15/min=30,/rootPath/year=2017/month=10/day=10/hour=15/min=45,/rootPath/year=2017/month=10/day=10/hour=16/min=00,/rootPath/year=2017/month=10/day=10/hour=16/min=15,/rootPath/year=2017/month=10/day=10/hour=16/min=30,/rootPath/year=2017/month=10/day=10/hour=16/min=45,/rootPath/year=2017/month=10/day=10/hour=17/min=00,/rootPath/year=2017/month=10/day=10/hour=17/min=15,/rootPath/year=2017/month=10/day=10/hour=17/min=30,/rootPath/year=2017/month=10/day=10/hour=17/min=45,/rootPath/year=2017/month=10/day=10/hour=18/min=00,/rootPath/year=2017/month=10/day=10/hour=18/min=15,/rootPath/year=2017/month=10/day=10/hour=18/min=30,/rootPath/year=2017/month=10/day=10/hour=18/min=45,/rootPath/year=2017/month=10/day=10/hour=19/min=00,/rootPath/year=2017/month=10/day=10/hour=19/min=15,/rootPath/year=2017/month=10/day=10/hour=19/min=30,/rootPath/year=2017/month=10/day=10/hour=19/min=45,/rootPath/year=2017/month=10/day=10/hour=20/min=00,/rootPath/year=2017/month=10/day=10/hour=20/min=15,/rootPath/year=2017/month=10/day=10/hour=20/min=30,/rootPath/year=2017/month=10/day=10/hour=20/min=45,/rootPath/year=2017/month=10/day=10/hour=21/min=00,/rootPath/year=2017/month=10/day=10/hour=21/min=15,/rootPath/year=2017/month=10/day=10/hour=21/min=30,/rootPath/year=2017/month=10/day=10/hour=21/min=45,/rootPath/year=2017/month=10/day=10/hour=22/min=00,/rootPath/year=2017/month=10/day=10/hour=22/min=15,/rootPath/year=2017/month=10/day=10/hour=22/min=30,/rootPath/year=2017/month=10/day=10/hour=22/min=45,/rootPath/year=2017/month=10/day=10/hour=23/min=00,/rootPath/year=2017/month=10/day=10/hour=23/min=15,/rootPath/year=2017/month=10/day=10/hour=23/min=30,/rootPath/year=2017/month=10/day=10/hour=23/min=45,/rootPath/year=2017/month=10/day=11/hour=00/min=00"
    assert(finalPath === expectedFinalPath)


  }
}
