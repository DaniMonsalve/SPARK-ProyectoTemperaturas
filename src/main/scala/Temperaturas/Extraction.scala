package Temperaturas

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, DataFrame}
import java.time.LocalDate
import scala.math.BigDecimal.RoundingMode // Importa RoundingMode para el redondeo
import org.apache.spark.sql.functions._

object CommonTypes {
  type Temperature = Double // °F, luego se transforma en Cº
  type Year = Int // Año calendario
  case class Location(latitude: Double, longitude: Double) // Coordenadas de la estación
}

/** [1]
 * Extractor de datos
 */
object Extraction {

  import CommonTypes._

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN) // Configura el nivel de log para Apache Spark

  val spark: SparkSession = SparkSession.builder()
    .appName("Temperaturas")
    .master("local[*]") // Usa todos los núcleos disponibles
    .getOrCreate()

  import spark.implicits._

  /** [2]
   * La siguiente parte del código carga el recurso desde el sistema de archivos como RDD[String]
   * @param resource
   *   la ruta del recurso
   * @return
   *   el contenido del recurso como RDD[String]
   */
  /**
   * La función getRDDFromResource está diseñada para cargar un recurso (como un archivo de texto) desde el sistema de archivos y convertir su contenido en un RDD[String].
   * @param resource Ruta del recurso a cargar.
   * @return El contenido del recurso como RDD[String].
   */
  def getRDDFromResource(resource: String): RDD[String] = {
    val fileStream = getClass.getResourceAsStream(resource) // Obtiene el flujo de entrada del recurso
    spark.sparkContext.makeRDD(
      scala.io.Source.fromInputStream(fileStream).getLines().toSeq
    ) // Convierte las líneas del archivo en un RDD
  }

  /** [3]
   * @param year             Número del año
   * @param stationsFile     Ruta del archivo de recursos de estaciones a usar (por ejemplo, "/stations.csv")
   * @param temperaturesFile Ruta del archivo de recursos de temperaturas a usar (por ejemplo, "/1975.csv")
   * @return                 Una secuencia que contiene tripletas (fecha, ubicación, temperatura)
   */
  /**
   * La función locateTemperatures combina datos de estaciones meteorológicas y lecturas de temperatura basadas en una clave común (STN, WBAN). 
   * Procesa los datos para asegurarse de que solo se usan las entradas válidas, convierte las temperaturas de Fahrenheit a Celsius, y devuelve una colección de tripletas que contienen la fecha, la ubicación y la temperatura en Celsius.
   * @param year Año para el cual se desean obtener los datos de temperatura.
   * @param stationsFile Ruta del archivo que contiene la información de las estaciones meteorológicas.
   * @param temperaturesFile Ruta del archivo que contiene las lecturas de temperatura.
   * @return Una secuencia de tripletas (LocalDate, Location, Temperature), donde:
   *         LocalDate es la fecha de la lectura.
   *         Location es la ubicación de la estación.
   *         Temperature es la temperatura en Celsius.
   */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): RDD[(LocalDate, Location, Temperature)] = {
    /* Necesitamos crear Pair RDDs para unirlos en sus pares (STN, WBAN) */
    val statRDD: RDD[((String, String), Location)] =
      getRDDFromResource(stationsFile) // carga el archivo de estaciones y lo convierte en un RDD[String].
        .map(line => line.split(",", -1)) // Divide cada línea del archivo en campos separados por comas. La opción -1 evita la eliminación de "" al final
        .filter {
          case Array(_, _, "", _) | Array(_, _, _, "") => false // Filtra estaciones con coordenadas GPS faltantes
          case _ => true
        }
        .map(a => {
          // Redondear las coordenadas a 2 decimales
          val latitude = BigDecimal(a(2)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
          val longitude = BigDecimal(a(3)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
          (
            (a(0), a(1)),
            Location(latitude, longitude)
          )
        })

    val tempRDD: RDD[((String, String), (LocalDate, Double))] =
      getRDDFromResource(temperaturesFile) // Carga el archivo de temperaturas y lo convierte en un RDD[String].
        .map(line => line.split(",", -1)) // Divide cada línea del archivo en campos separados por comas. evita la eliminación de "" al final
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
    val joinedRDD: RDD[(LocalDate, Location, Temperature)] =
      statRDD.join(tempRDD) // Une los RDDs por (STN, WBAN)
        .map {
          case (_, (loc, (date, temp))) => (date, loc, temp)
        }

    joinedRDD
  }

  /**
   * La función locationYearlyAverageRecords toma un conjunto de datos de temperaturas diarias para diferentes ubicaciones y años, 
   * y calcula la temperatura promedio anual para cada ubicación. 
   * @param records Una colección de tuplas (LocalDate, Location, Temperature) que representan las lecturas de temperatura diaria para diferentes ubicaciones y años.
   * @return Una colección de tuplas (Year, Location, Temperature) que representan la temperatura promedio anual para cada ubicación.
   */
  def locationYearlyAverageRecords(records: RDD[(LocalDate, Location, Temperature)]): RDD[(Year, Location, Temperature)] = {
    
    // Mapea las entradas originales (fecha, ubicación, temperatura) a una clave compuesta por (año, ubicación).
    // Para cada clave, se crea un valor compuesto por (temperatura, 1) para contar el número de registros.
    val mappedRecords = records.map {
      case (date, loc, temp) => ((date.getYear, loc), (temp, 1))
    }
    
    // Reduce los datos sumando las temperaturas y el conteo de registros por cada (año, ubicación).
    // La función reduceByKey combina las temperaturas y los contadores asociados a cada clave (año, ubicación).
    val reducedRecords = mappedRecords.reduceByKey {
      case ((tempSum1, count1), (tempSum2, count2)) => (tempSum1 + tempSum2, count1 + count2)
    }
    
    // Calcula la temperatura promedio dividiendo la suma total de temperaturas entre el número de registros.
    // Utiliza BigDecimal para manejar la precisión y redondea el resultado a dos decimales.
    val averagedRecords = reducedRecords.mapValues {
      case (tempSum, count) => BigDecimal(tempSum / count).setScale(2, RoundingMode.HALF_UP).toDouble
    }
    
    // Finalmente, mapea los resultados de vuelta a la estructura original: (año, ubicación, temperatura promedio).
    averagedRecords.map {
      case ((year, loc), avgTemp) => (year, loc, avgTemp)
    }
  }

  def main(args: Array[String]): Unit = {
    val stationsFile = "/stations.csv"
    val startYear = 1975
    val endYear = 1976
    val temperatureFiles = (startYear to endYear).map(year => s"/$year.csv")

    try {
      // Procesar los archivos de temperatura por año
      val yearlyRDDs = temperatureFiles.map { file =>
        val year = file.stripPrefix("/").stripSuffix(".csv").toInt
        locateTemperatures(year, stationsFile, file).map {
          case (date, loc, temp) => (year, loc, temp)
        }
      }

      // Unir todos los RDDs de los años
      val allData = yearlyRDDs.reduce(_ union _)

      // Calcular el promedio anual por ubicación
      val result = locationYearlyAverageRecords(allData.map {
        case (year, loc, temp) => (LocalDate.of(year, 1, 1), loc, temp)
      })

      // Convertir el RDD a DataFrame con nombres de columnas
      import spark.implicits._ // Importar implicits para la conversión a DataFrame
      val df = result.toDF("year", "long_lat", "temperatura_media")

      // Extraer latitud y longitud de la columna 'long_lat' y crear nuevas columnas
      val dfWithCoordinates = df.withColumn("latitud", $"long_lat.latitude")
                                .withColumn("longitud", $"long_lat.longitude")

      // Ordenar el DataFrame por latitud y longitud
      val dfOrdered = dfWithCoordinates.orderBy("longitud", "latitud")

      // Mostrar las primeras 20 filas del DataFrame ordenado
      dfOrdered.show(20, false) // 'false' para no truncar el contenido

      // Recoger todas las filas del DataFrame ordenado y mostrar las primeras 20
      val allRows = dfOrdered.collect()
      allRows.take(20).foreach(println)

      // Opcional: Mostrar el tamaño del DataFrame ordenado
      println(s"Número de filas del DataFrame: ${dfOrdered.count()}")

    } catch {
      case e: Exception =>
        println(s"Se produjo un error: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
