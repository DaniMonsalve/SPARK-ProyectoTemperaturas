# Proyecto Análisis de Temperaturas Históricas

Este proyecto utiliza Apache Spark para procesar y analizar datos históricos de temperaturas. Con este proyecto, puedes calcular la temperatura media anual por ubicación a partir de archivos de datos que contienen información de estaciones meteorológicas y lecturas de temperatura.

## Contenido

1. [Descripción](#descripción)
2. [Estructura del Proyecto](#estructura-del-proyecto)
3. [Requisitos](#requisitos)
4. [Resumen del Código `Extraction.scala`](#resumen-del-código-extractionscala)
5. [Resumen del Código `ResourceTest.scala`](#resumen-del-código-testscala)
6. [Ejecutar el Proyecto y Resultados](#ejecutar-el-proyecto-y-resultados)


## 1. Descripción

Este proyecto carga datos de estaciones meteorológicas y temperaturas desde archivos CSV, procesa estos datos para calcular la temperatura promedio anual por ubicación, y presenta los resultados en un formato ordenado.

Spark Session corre en local.

El orden de las tareas es el siguiente:

1. **Carga de Datos:** Lee archivos CSV que contienen información sobre estaciones meteorológicas y temperaturas.
2. **Transformación de Datos:** Convierte las temperaturas de Fahrenheit a Celsius y filtra datos inválidos.
3. **Cálculo de Promedios:** Calcula la temperatura promedio anual para cada ubicación.
4. **Visualización:** Presenta los resultados en un DataFrame ordenado por latitud y longitud.

## 2. Estructura del Proyecto

El proyecto está organizado en las siguientes carpetas:

- **`src/`**: Contiene el código fuente y los recursos.
  - `src/main/scala/`: Código fuente en Scala.
  - `src/main/resources/`: Archivos de recursos como CSV.
  - `src/test/scala/`: Código para pruebas.
- **`build.sbt`**: Configuración principal de SBT.
- **`project/`**: Configuraciones adicionales de SBT (opcional).

## 3. Requisitos

- **Apache Spark**: Para ejecutar el procesamiento distribuido.
- **SBT**: Para compilar y ejecutar el proyecto.
- **Java**: Necesario para ejecutar Spark.

## 4. Resumen del Código `Extraction.scala`

El archivo `Extraction.scala` contiene la lógica principal del proyecto y se encarga de:

1. **Configuración Inicial:**
   - Crea una instancia de `SparkSession` para permitir el procesamiento distribuido con Spark.
   - Configura el nivel de registro de Spark para reducir la cantidad de logs mostrados.

2. **Carga de Datos:**
   - La función `getRDDFromResource` carga archivos CSV desde el sistema de archivos y los convierte en RDD de cadenas de texto (`RDD[String]`).

3. **Procesamiento de Datos:**
   - **`locateTemperatures`:** 
     - Carga los archivos de estaciones y temperaturas.
     - Filtra datos inválidos y convierte las temperaturas de Fahrenheit a Celsius.
     - Une los datos de estaciones y temperaturas basados en identificadores comunes (STN, WBAN).
   - **`locationYearlyAverageRecords`:** 
     - Agrupa las temperaturas por año y ubicación.
     - Calcula la temperatura promedio anual para cada ubicación.

4. **Visualización:**
   - Convierte el RDD resultante en un DataFrame.
   - Extrae latitud y longitud del DataFrame y lo ordena por estas coordenadas.
   - Muestra los resultados ordenados en la consola.

## 5. Resumen del Código `TestResource.scala`

El archivo `TestResource.scala` tiene como objetivo:

1. **Pruebas de Recursos:**
   - **Verificación de Recursos:** El código en `TestResource.scala` está diseñado para verificar la disponibilidad de recursos dentro del proyecto. En este caso, se enfoca en la carga y lectura del archivo de recursos `stations.csv`.

2. **Comprobación de Accesibilidad:**
   - **Carga del Recurso:** Utiliza el método `getClass.getResourceAsStream` para intentar acceder al recurso `stations.csv` ubicado en el directorio `resources`.
   - **Verificación de Existencia:** Si el flujo de entrada (`stream`) es `null`, el archivo no se encuentra en la ruta especificada, y se imprime un mensaje de error. Si el flujo no es `null`, el recurso está disponible y se lee su contenido.

3. **Lectura y Presentación de Contenidos:**
   - **Lectura del Contenido:** Lee el contenido del archivo de recursos utilizando `scala.io.Source.fromInputStream` y lo convierte en una cadena de texto.
   - **Impresión de Contenido:** Muestra el contenido del archivo en la consola para que el usuario pueda verificar visualmente que el archivo se está cargando correctamente.

Este archivo es útil para confirmar que los archivos de recursos están correctamente incluidos en el proyecto y son accesibles en tiempo de ejecución. No realiza pruebas unitarias, sino que simplemente verifica la presencia y el contenido de los archivos de recursos.

## 6. Ejecutar el Proyecto y Resultados

1. **Preparación del Entorno:**
   - Asegúrate de tener Java, Apache Spark, y SBT instalados y configurados en tu sistema.

2. **Ejecución del Proyecto:**
   - Clona el repositorio y navega a la carpeta del proyecto:
     ```bash
     git clone https://github.com/tu_usuario/tu_repositorio.git
     cd tu_repositorio
     ```
   - Compila el proyecto:
     ```bash
     sbt compile
     ```
   - Ejecuta el proyecto:
     ```bash
     sbt run
     ```

3. **Resultados:**
   - El programa procesará los archivos CSV de temperaturas y estaciones, calculará las temperaturas promedio anuales y las presentará en la consola en forma de DataFrame ordenado por latitud y longitud.

