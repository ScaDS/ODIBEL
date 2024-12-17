package ai.scads.odibel.datasets.wikitext.eval

import scala.collection.mutable

class FunctionRegistry {

  private val functions = mutable.Map[String, () => Any]()

  def register[R](name: String)(block: => R): Unit = {
    functions.put(name, () => block)
  }

  def execute[R](name: String): R = {
    functions.get(name) match {
      case Some(func) => func().asInstanceOf[R]
      case None => throw new NoSuchElementException(s"Function '$name' is not registered.")
    }
  }
}

object FunctionRegistry extends App {

  val reg = new FunctionRegistry()

  reg.register("some") {
    println("foobar")
  }

  reg.execute[Unit]("some")
  reg.execute[Unit]("some")
}
