package flights

import org.apache.spark.sql.Row
import scala.util.Try

object FlightsParser {

  /**
   * Cleans strings: handles null, removes spaces and converts to lowercase.
   */
  private def cleanStr(value: Any, toLower: Boolean = true): String = {
    val s = Option(value).map(_.toString.trim).getOrElse("")
    if (s.isEmpty) "unknown"
    else if (toLower) s.toLowerCase
    else s
  }

  /**
   * Cleans numeric values: handles null and invalid values by returning 0.0.
   */
  private def cleanFloat(value: Any): Double = {
    Option(value)
      .map(_.toString)
      .flatMap(s => Try(s.toDouble).toOption)
      .getOrElse(0.0)
  }

  /**
   * Parses an airport record into a flat tuple.
   * Output: (id, name, city, state)
   */
  def parseAirports(row: Row): Option[(String, String, String, String)] = {
    Try {
      val airportId = cleanStr(row(0))
      val airportName = cleanStr(row(5))
      val city = cleanStr(row(3))
      val state = cleanStr(row(4))

      if (airportId == "unknown") throw new Exception("Invalid ID")

      (airportId, airportName, city, state)
    }.toOption
  }

  /**
   * Parses a flight record.
   * Output: (origin_id, airline, year, month, arr_delay, air_delay, weath_delay, nas_delay, sec_delay, late_delay)
   */
  def parseFlights(row: Row): Option[(String, String, Int, Int, Double, Double, Double, Double, Double, Double)] = {
    Try {
      val originId = cleanStr(row(8))
      val airline = cleanStr(row(5))
      val year = cleanFloat(row(0)).toInt
      val month = cleanFloat(row(1)).toInt

      if (originId == "unknown" || airline == "unknown") throw new Exception("Invalid Data")

      // Delay cleaning
      val arrDelay = Math.max(0.0, cleanFloat(row(24)))
      val airlineDelay = cleanFloat(row(29)) // row(29) is airline_delay
      val weathDelay = cleanFloat(row(30))
      val nasDelay = cleanFloat(row(31))
      val secDelay = cleanFloat(row(32))
      val lateDelay = cleanFloat(row(33))

      (
        originId, airline, year, month,
        arrDelay, airlineDelay, weathDelay,
        nasDelay, secDelay, lateDelay
      )
    }.toOption
  }
}