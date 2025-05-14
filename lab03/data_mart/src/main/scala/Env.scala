import scala.io.Source

object Env {
  private val envMap: Map[String, String] = {
    val source = Source.fromFile(".env")
    val lines = try {
      source.getLines().toList
    } finally {
      source.close()
    }

    lines
      .filter(line => line.contains("=") && !line.trim.startsWith("#"))
      .map { line =>
        val Array(key, value) = line.split("=", 2)
        key.trim -> value.trim.replaceAll("\"", "")
      }
      .toMap
  }

  def get(key: String): String = envMap.getOrElse(key, throw new RuntimeException(s"Missing env key: $key"))
  def getOrElse(key: String, default: String): String = envMap.getOrElse(key, default)
}