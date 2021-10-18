import com.tfs.dp.spartan.Spartan
import com.tfs.dp.spartan.dg.DGCatalogModel.{ImportStatic, SQL, Serde}
import com.tfs.dp.spartan.dg.DGCatalogRegistry
import com.tfs.dp.spartan.manager.TableManager

/**
  * Created by guruprasad.gv on 8/31/17.
  */
object TestCatalog {

  def main(args: Array[String]): Unit = {
    val spartan: Spartan = new Spartan("/Users/guruprasad.gv/sdata")
    println(spartan.catalog());



    println(spartan.define("ResolutionReport"))
    println(spartan.define("RawIDM"))


    //All IDM events
    DGCatalogRegistry.add("ALLChannelInteractions", "/raw/prod/rtdp/idm/events", Serde.AVRO,Map.empty)

    //Chat Interactions
    DGCatalogRegistry.add("ChatInteractions", "/raw/prod/pxassist/interactions", Serde.ASSIST_INTERACTIONS,Map.empty)
    
    //Chat AgentStats
    DGCatalogRegistry.add("ChatAgentStats", "/raw/prod/pxassist/agentstats", Serde.ASSIST_AGENTSTATS,Map.empty)

    //V2_Chat Interactions
    DGCatalogRegistry.add("V2ChatInteractions", "/raw/prod/pxassist/interactions/v2", Serde.ASSIST_INTERACTIONS_V2,Map.empty)

    //"Online" Interactions
    DGCatalogRegistry.add("OnlineInteractions", "/views/OnlineInteractions", Serde.AVRO,
      Seq(
        new ImportStatic("ALLChannelInteractions", null, null)),
      Seq(
        new SQL("ChatInteractions","""
                                SELECT * from ALLChannelInteractions where header.channel == 'ONLINE'
                        """)
      ),
      Map.empty,
      Map.empty
    )

    //Speech Interactions
    DGCatalogRegistry.add("SpeechInteractions", "/views/SpeechInteractions", Serde.AVRO,
      Seq(
        new ImportStatic("ALLChannelInteractions", null, null)),
      Seq(
        new SQL("SpeechInteractions","""
                                SELECT * from ALLChannelInteractions
                                where header.channel == 'SPEECH'
                        """)
      ),
      Map.empty,
      Map.empty
    )
    // "Omni" Interactions
    DGCatalogRegistry.add("OmniInteractions", "/views/OmniInteractions", Serde.AVRO,
      Seq(
        new ImportStatic("ALLChannelInteractions", null, null)),
      Seq(
        new SQL("OmniInteractions","""
                                SELECT * from ALLChannelInteractions
                                where header.channel == 'OMNICHANNEL'
                        """)
      ),
      Map.empty,
      Map.empty
    )
    // "Web Speec Interactions"
    DGCatalogRegistry.add("WebSpeechInteractions", "/views/WebSpeechInteractions", Serde.AVRO,
      Seq(
        new ImportStatic("ALLChannelInteractions", null, null)),
      Seq(
        new SQL("WebSpeechInteractions","""
                                SELECT * from ALLChannelInteractions
                                where header.channel == 'SPEECH' or header.channel == 'WEB'
                        """)
      ),
      Map.empty,
      Map.empty
    )


    println(spartan.define("WebSpeechInteractions"))

    val table: TableManager = spartan.use("ChatInteractions", "cap1enterprise", "201807150000", "201807150000")

    println(table.schema());
    table.df().show(10, false)
  }
}
