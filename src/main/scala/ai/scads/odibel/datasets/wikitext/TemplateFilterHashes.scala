package ai.scads.odibel.datasets.wikitext

import ai.scads.odibel.datasets.wikitext.WikiUtil.Hashes
import org.apache.spark.sql.SparkSession

import java.util.Base64
import java.security.MessageDigest

object TemplateFilterHashes extends App {

  val tfh = new TemplateFilterHashes()

  tfh.run("/home/marvin/workspace/data/wikidumps/head10000.split.ns=0","[iI]nfobox")
}


class TemplateFilterHashes {


  def run(in: String, templateRegexUnEsc: String): Unit = {

    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    val sparkSql = spark.sqlContext
    import sparkSql.implicits._
    import Hashes._

    spark.read.json(in).as[FlatRawPageRevision].map(WikiUtil.mapSplitElementToRevisionElement)
      .map({
        revision =>
          val iboxes = WikiTextParser.extractTemplatesStartingWith("[iI]nfobox", revision.text)
            .map(ibox => (ibox.templateName, Hashes.md5(ibox.content).toBase64, ibox.content.replaceAll("\n","\\n")))
          RevisionTemplateInfo(revision.pId, revision.rId, iboxes)
      }).filter(_.templates.nonEmpty).show(truncate = false)
  }
}
