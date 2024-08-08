name := "Temperaturas"

version := "0.1"

scalaVersion := "2.12.17"  // Puedes usar "2.13.11" si es necesario, pero asegúrate de que todas las dependencias sean compatibles

scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-encoding", "UTF-8",
  "-unchecked"
)

libraryDependencies ++= Seq(
  // Dependencias de Apache Spark
  "org.apache.spark" %% "spark-core" % "3.3.0",
  "org.apache.spark" %% "spark-sql" % "3.3.0",
  
  // Dependencias de Akka
  "com.typesafe.akka" %% "akka-stream" % "2.6.18",
  "com.typesafe.akka" %% "akka-stream-testkit" % "2.6.18",
  
  // Dependencias para Monix y FS2
  "io.monix" %% "monix" % "3.4.0",
  "co.fs2" %% "fs2-io" % "2.5.6",
  
  // Dependencias para pruebas
  "org.scalacheck" %% "scalacheck" % "1.15.4" % Test,
  "org.scalameta" %% "munit" % "0.7.26" % Test,
  
  // Dependencias para Log4j
  "org.apache.logging.log4j" % "log4j-core" % "2.17.2",
  "org.apache.logging.log4j" % "log4j-api" % "2.17.2"
)

resolvers += "Maven Central" at "https://repo1.maven.org/maven2/"

// Configuración para excluir dependencias no deseadas
def excludes(m: ModuleID): ModuleID =
  m.exclude("io.netty", "netty-common").
    exclude("io.netty", "netty-handler").
    exclude("io.netty", "netty-transport").
    exclude("io.netty", "netty-buffer").
    exclude("io.netty", "netty-codec").
    exclude("io.netty", "netty-resolver").
    exclude("io.netty", "netty-transport-native-epoll").
    exclude("io.netty", "netty-transport-native-unix-common").
    exclude("javax.xml.bind", "jaxb-api").
    exclude("jakarta.xml.bind", "jaxb-api").
    exclude("javax.activation", "activation").
    exclude("jakarta.annotation", "jakarta.annotation-api").
    exclude("javax.annotation", "javax.annotation-api").
    exclude("org.slf4j", "slf4j-log4j12").
    exclude("com.google.protobuf", "protobuf-java")

mainClass in Compile := Some("Temperaturas.Main")

// Incluir las carpetas de recursos en el classpath
Test / unmanagedResourceDirectories += baseDirectory.value / "src" / "main" / "resources"
