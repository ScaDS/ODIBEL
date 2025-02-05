package ai.scads.odibel.datasets.wikitext.utils

import java.io.File
import scala.io.Source

object WikiTextParser extends App {

  case class SimpleTemplateUsage(templateName: String, content: String)

  // TODO dont know if it is a problem to match on prefix
  //  and do not check if it has a longer name in some cases
  def extractTemplatesStartingWith(prefix: String, wikitext: String): List[SimpleTemplateUsage] = {
    val reg = s"\\{\\{[\\s\\n]*$prefix".r

    // Apply the regex and parse the nested content
    reg.findAllMatchIn(wikitext).flatMap { regexMatch =>
      val name = parseTemplateName(wikitext, regexMatch.start)
      val content = parseNestedBraces(wikitext, regexMatch.start)
      if (content.isEmpty) None
      else Some(SimpleTemplateUsage(name.get, content.get))
    }.toList
  }

  // Simplified function to parse the template name using regex
  def parseTemplateName(s: String, start: Int): Option[String] = {
    val substring = s.substring(start)
    val templateNamePattern = """\{\{\s*([^\|\{\}]+)""".r
    templateNamePattern.findFirstMatchIn(substring) match {
      case Some(m) => Some(m.group(1).trim)
      case None => None
    }
  }

  // Function to parse nested braces
  def parseNestedBraces(s: String, start: Int): Option[String] = {
    val sb = new StringBuilder()
    var pos = start
    var left = 0
    var right = 0
    val len = s.length

    while (pos < len && (left == 0 || left != right)) {
      if (s.startsWith("{{", pos)) {
        left += 1
        sb.append("{{")
        pos += 2
      } else if (s.startsWith("}}", pos)) {
        right += 1
        sb.append("}}")
        pos += 2
      } else {
        sb.append(s.charAt(pos))
        pos += 1
      }
    }

    if (left != right) {
      None
    } else {
      Some(sb.toString())
    }
  }

  val source = Source.fromFile(new File("tmp/Leipzig"))

  extractTemplatesStartingWith("[iI]nfobox", source.getLines().mkString("\n")).foreach(x => println(x.templateName))

  source.close()
}


//  // Function to parse the full template name
//  def parseTemplateName(s: String, start: Int): String = {
//    val sb = new StringBuilder()
//    var pos = start
//    val len = s.length
//    var braceCount = 0
//    var done = false
//
//    // Skip initial braces
//    while (pos < len && s.charAt(pos) == '{') {
//      sb.append(s.charAt(pos))
//      pos += 1
//      braceCount += 1
//    }
//
//    // Skip any whitespace
//    while (pos < len && s.charAt(pos).isWhitespace) {
//      sb.append(s.charAt(pos))
//      pos += 1
//    }
//
//    // Collect the template name until we hit a delimiter (|) or closing braces (}})
//    while (pos < len && !done) {
//      if (s.startsWith("}}", pos) || s.startsWith("|", pos)) {
//        done = true
//      } else if (s.startsWith("{{", pos)) {
//        // Handle nested templates in the name (if any)
//        val nestedName = parseNestedBraces(s, pos)
//        sb.append(nestedName)
//        pos += nestedName.length
//      } else {
//        sb.append(s.charAt(pos))
//        pos += 1
//      }
//    }
//
//    sb.toString().trim
//  }
