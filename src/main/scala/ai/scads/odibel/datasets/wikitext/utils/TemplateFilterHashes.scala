package ai.scads.odibel.datasets.wikitext.utils

import WikiUtil.Hashes
import ai.scads.odibel.datasets.wikitext.data.{PageRevisionXmlSplit, RevisionTemplateInfo}
import org.apache.spark.sql.SparkSession

// TODO write documentation
object TemplateFilterHashes extends App {

  val tfh = new TemplateFilterHashes()

  tfh.run("/home/marvin/workspace/data/wikidumps/head10000.split.ns=0","[iI]nfobox")
}


class TemplateFilterHashes {

  def run(in: String, templateRegexUnEsc: String): Unit = {

    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    val sparkSql = spark.sqlContext
    import Hashes._
    import sparkSql.implicits._

    spark.read.json(in).as[PageRevisionXmlSplit].map(WikiUtil.mapSplitElementToRevisionElement)
      .map({
        revision =>
          val iboxes = WikiTextParser.extractTemplatesStartingWith("[iI]nfobox", revision.text)
            .map(ibox => (ibox.templateName, Hashes.md5(ibox.content).toBase64, ibox.content.replaceAll("\n","\\n")))
          RevisionTemplateInfo(revision.pId, revision.rId, iboxes)
      }).filter(_.templates.nonEmpty).show(truncate = false)
  }
}
