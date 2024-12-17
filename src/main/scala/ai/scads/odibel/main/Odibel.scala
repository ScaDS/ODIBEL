package ai.scads.odibel.main

import picocli.AutoComplete.GenerateCompletion
import picocli.CommandLine.{Command, Option}

import java.util.concurrent.Callable

@Command(name = "odibel", mixinStandardHelpOptions = true, version = Array("alpha-0.1"), subcommands = Array(classOf[GenerateCompletion]))
class Odibel extends Callable[Int] {

  @Option(names = Array("--query","-q"), required = true)
  var query: String = _

  @Option(names = Array("--selection","-s"), required = true)
  var selection: String = _

  @Option(names = Array("--no-cache","-nq"), required = false)
  var noCache: Boolean = _

  override def call(): Int = {

    println(Array(query,selection,noCache))
    /* exit code */ 0
  }
}
