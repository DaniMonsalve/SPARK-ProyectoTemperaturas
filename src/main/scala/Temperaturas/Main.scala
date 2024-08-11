package Temperaturas


import scala.util.Properties.isWin

object Main extends App {
  // Configura el entorno Hadoop para Windows si es necesario
  if (isWin) System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "\\winutils\\hadoop-3.3.1")

  // Llama a Extraccion para realizar su trabajo
  Extraccion.main(Array.empty)  // Llama al método main de Extraccion, si existe
}

