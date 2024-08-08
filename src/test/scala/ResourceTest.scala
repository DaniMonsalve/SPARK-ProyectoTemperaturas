package Temperaturas

object ResourceTest {
  def main(args: Array[String]): Unit = {
    val resource = "/stations.csv"
    val stream = getClass.getResourceAsStream(resource)
    
    if (stream == null) {
      println(s"Resource not found: $resource")
    } else {
      println(s"Resource found: $resource")
      val source = scala.io.Source.fromInputStream(stream)
      val lines = source.getLines().mkString("\n")
      println("Resource contents:")
      println(lines)
      source.close()
    }
  }
}
