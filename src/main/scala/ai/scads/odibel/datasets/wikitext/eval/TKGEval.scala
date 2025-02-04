package ai.scads.odibel.datasets.wikitext.eval


import picocli.CommandLine.Command

import java.util.concurrent.Callable

/**
 * A single Callable to execute all
 * This feature was removed and the calls are done by a script
 */
@Command(name = "eval", mixinStandardHelpOptions = true, subcommands = Array(classOf[InputEval], classOf[SnapshotEval], classOf[OutputEval]))
class TKGEval extends Callable[Int] {

  override def call(): Int = {
    0
  }
}
