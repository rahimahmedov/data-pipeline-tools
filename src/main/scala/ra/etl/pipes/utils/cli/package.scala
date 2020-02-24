package ra.etl.pipes.utils

package object cli {


  case class ArgumentMap(args: Array[String]) {

    private def addElements(strings: List[String]): Map[String, String] = strings match {
      case h1 :: h2 :: t => Map(h1 -> h2) ++ addElements(t)
      case _ => Map.empty
    }

    private val argMap = addElements(args.toList)

    def getOrElse(k: String, v: String): String = argMap.getOrElse(k, v)

  }

  implicit def toArgMap(argList: Array[String]): ArgumentMap = ArgumentMap(argList)

}
