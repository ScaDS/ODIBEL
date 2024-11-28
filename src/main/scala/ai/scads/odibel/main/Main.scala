package ai.scads.odibel.main

import picocli.CommandLine
import picocli.CommandLine.Command

import java.util.concurrent.Callable

object Main extends App {
  val preparedArgs = if(args.length == 0) Array("-h") else args
  val exitCode = new CommandLine(new Main()).execute(preparedArgs: _*)
  System.exit(exitCode)
}

@Command(
  name="odibel",
  subcommands = Array(classOf[DBpediaTKG]),
  mixinStandardHelpOptions = true
)
class Main extends Callable[Int] {

  override def call(): Int = {
    0
  }
}