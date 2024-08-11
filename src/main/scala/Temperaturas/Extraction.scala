package Temperaturas

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, DataFrame}
import java.time.LocalDate
import scala.math.BigDecimal.RoundingMode
import org.apache.spark.sql.functions._
import java.nio.file.{Files, Paths}
import org.apache.spark.storage.StorageLevel

object TiposComunes {
  type Temperatura = Double // °F, luego se transforma a °C
  type Year = Int // Año calendario
  case class Ubicacion(latitud: Double, longitud: Double) // Coordenadas de la estación
}

/** [1]
 * Extractor de datos
 */
object Extraccion {

  import TiposComunes._

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN) // Configura el nivel de log para Apache Spark

  val spark: SparkSession = SparkSession.builder()
    .appName("Temperaturas")
    .master("local[*]") // Usa todos los núcleos disponibles
    .getOrCreate()

  // Habilitar la ejecución adaptativa de consultas
  spark.conf.set("spark.sql.adaptive.enabled", true) // Ajusta dinámicamente la ejecución de las consultas según los datos procesados en tiempo de ejecución.
  spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", true) // Reduce el número de particiones en la etapa de shuffle si es posible.
  spark.conf.set("spark.sql.adaptive.skewJoin.enabled", true) // Maneja uniones desbalanceadas.

  // Configurar la propiedad para desactivar la verificación de acceso a archivos
  spark.conf.set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")

  import spark.implicits._

  /** [2]
   * La siguiente parte del código carga el recurso desde el sistema de archivos como RDD[String]
   * @param recurso
   *   la ruta del recurso
   * @return
   *   el contenido del recurso como RDD[String]
   */
  def obtenerRDDDesdeRecurso(recurso: String): RDD[String] = {
    val flujoArchivo = getClass.getResourceAsStream(recurso) // Obtiene el flujo de entrada del recurso
    spark.sparkContext.makeRDD(
      scala.io.Source.fromInputStream(flujoArchivo).getLines().toSeq
    ) // Convierte las líneas del archivo en un RDD
  }

  /** [3]
   * @param year                Número del año
   * @param archivoEstaciones  Ruta del archivo de recursos de estaciones a usar (por ejemplo, "/stations.csv")
   * @param archivoTemperaturas Ruta del archivo de recursos de temperaturas a usar (por ejemplo, "/1975.csv")
   * @return                  Una secuencia que contiene tripletas (fecha, ubicación, temperatura)
   */
  def localizarTemperaturas(year: Year, archivoEstaciones: String, archivoTemperaturas: String): RDD[(LocalDate, Ubicacion, Temperatura)] = {
    /* Necesitamos crear Pair RDDs para unirlos en sus pares (STN, WBAN) */
    val rddEstaciones: RDD[((String, String), Ubicacion)] =
      obtenerRDDDesdeRecurso(archivoEstaciones) // carga el archivo de estaciones y lo convierte en un RDD[String].
        .map(linea => linea.split(",", -1)) // Divide cada línea del archivo en campos separados por comas. La opción -1 evita la eliminación de "" al final
        .filter {
          case Array(_, _, "", _) | Array(_, _, _, "") => false // Filtra estaciones con coordenadas GPS faltantes
          case _ => true
        }
        .map(a => {
          // Redondear las coordenadas a 2 decimales
          val latitud = BigDecimal(a(2)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
          val longitud = BigDecimal(a(3)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
          (
            (a(0), a(1)),
            Ubicacion(latitud, longitud)
          )
        })

    val rddTemperaturas: RDD[((String, String), (LocalDate, Double))] =
      obtenerRDDDesdeRecurso(archivoTemperaturas) // Carga el archivo de temperaturas y lo convierte en un RDD[String].
        .map(linea => linea.split(",", -1)) // Divide cada línea del archivo en campos separados por comas. evita la eliminación de "" al final
        .map(a => // Convierte cada línea en una tupla.
          (
            (a(0), a(1)),
            (
              LocalDate.of(year, a(2).toInt, a(3).toInt), // Crea la fecha (año, mes, día)
              (a(4).toDouble - 32) * 5 / 9 // Aquí se convierte la temperatura de Fahrenheit a Celsius
            )
          )
        )

    // Unir los RDDs por las claves (STN, WBAN)
    val rddUnido: RDD[(LocalDate, Ubicacion, Temperatura)] =
      rddEstaciones.join(rddTemperaturas) // Une los RDDs por (STN, WBAN)
        .map {
          case (_, (ubicacion, (fecha, temp))) => (fecha, ubicacion, temp)
        }

    rddUnido
  }

  /**
    * La función promediarTemperaturasAnuales toma un conjunto de datos de temperaturas diarias para diferentes ubicaciones y años, 
    * y calcula la temperatura promedio anual para cada ubicación. 
    * @param registros Una colección de tuplas (LocalDate, Ubicacion, Temperatura) que representan las lecturas de temperatura diaria para diferentes ubicaciones y años.
    * @return Una colección de tuplas (Year, Ubicacion, Temperatura) que representan la temperatura promedio anual para cada ubicación.
    */
  def promediarTemperaturasAnuales(registros: RDD[(LocalDate, Ubicacion, Temperatura)]): RDD[(Year, Ubicacion, Temperatura)] = {
    // Mapea las entradas originales (fecha, ubicación, temperatura) a una clave compuesta por (año, ubicación).
    // Para cada clave, se crea un valor compuesto por (temperatura, 1) para contar el número de registros.
    val registrosMapeados: RDD[((Year, Ubicacion), (Temperatura, Int))] = registros.map {
      case (fecha, ubicacion, temperatura) => ((fecha.getYear, ubicacion), (temperatura, 1))
    }

    // Reduce los datos sumando las temperaturas y el conteo de registros por cada (año, ubicación).
    val registrosReducidos: RDD[((Year, Ubicacion), (Double, Int))] = registrosMapeados.reduceByKey {
      (a: (Double, Int), b: (Double, Int)) => 
        val (sumaTemp1, conteo1) = a
        val (sumaTemp2, conteo2) = b
        (sumaTemp1 + sumaTemp2, conteo1 + conteo2)
    }

    // Calcula la temperatura promedio dividiendo la suma total de temperaturas entre el número de registros.
    val registrosPromediados: RDD[((Year, Ubicacion), Double)] = registrosReducidos.mapValues {
      case (sumaTemp: Double, conteo: Int) =>
        BigDecimal(sumaTemp / conteo).setScale(2, RoundingMode.HALF_UP).toDouble
    }

    // Finalmente, mapea los resultados de vuelta a la estructura original: (año, ubicación, temperatura promedio).
    registrosPromediados.map {
      case ((year, ubicacion), tempPromedio) => (year, ubicacion, tempPromedio)
    }
  }

  def main(args: Array[String]): Unit = {
    val archivoEstaciones = "/stations.csv"
    val yearInicio = 1975
    val yearFin = 2015
    val archivosTemperaturas = (yearInicio to yearFin).map(year => s"/$year.csv")
    val rutaSalida = "output"

    // Crear el directorio de salida si no existe
    val ruta = Paths.get(rutaSalida)
    if (Files.notExists(ruta)) {
      Files.createDirectories(ruta)
    }

    try {
      // Procesar los archivos de temperatura por año y almacenar los resultados en disco
      archivosTemperaturas.foreach { archivo =>
        val year = archivo.stripPrefix("/").stripSuffix(".csv").toInt
        val datosAnuales = localizarTemperaturas(year, archivoEstaciones, archivo)
          .map {
            case (fecha, ubicacion, temperatura) => (year, ubicacion, temperatura)
          }

        // Calcular el promedio anual por ubicación para el año actual
        val resultado = promediarTemperaturasAnuales(datosAnuales.map {
          case (year, ubicacion, temperatura) => (LocalDate.of(year, 1, 1), ubicacion, temperatura)
        })

        // Convertir el RDD a DataFrame
        import spark.implicits._
        val df = resultado.toDF("year", "ubicacion", "temperatura_promedio")

        // Mostrar las primeras 10 filas del DataFrame para ver si hay datos
        df.show(10)

        // Extraer latitud y longitud de la columna 'ubicacion' y crear nuevas columnas
        val dfConCoordenadas = df.withColumn("latitud", $"ubicacion.latitud")
                                .withColumn("longitud", $"ubicacion.longitud")
                                .drop("ubicacion") // Opcionalmente elimina la columna 'ubicacion' si ya no es necesaria

        // Ordenar el DataFrame por latitud y longitud
        val dfOrdenado = dfConCoordenadas.orderBy("longitud", "latitud")

        // Persistir el DataFrame en disco utilizando StorageLevel.DISK_ONLY
        val dfPersistido = dfOrdenado.persist(StorageLevel.DISK_ONLY)

        // Guardar el DataFrame en disco
        dfPersistido.write
          .mode("overwrite")
          .option("header", "true")
          .csv(rutaSalida)

        // Una vez terminadas las operaciones necesarias, liberar el almacenamiento
        dfPersistido.unpersist()
      }

      println("Procesamiento completado con éxito.")

    } catch {
      case e: Exception =>
        println(s"Se produjo un error: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      // Este bloque se ejecutará siempre, incluso si ocurre una excepción.
      // Aquí es donde garantizas que Spark se detenga y libere sus recursos.
      spark.stop()
    }
  }
}
