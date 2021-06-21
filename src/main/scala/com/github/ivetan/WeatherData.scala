package com.github.ivetan

import kantan.csv.{HeaderEncoder, rfc}
import kantan.csv.ops.toCsvOutputOps
import upickle.default

import java.io.{File, PrintWriter}
import scala.xml.XML

object WeatherData extends App {

  val folderName = "./src/resources"
  val dataPath = "./src/resources/xml/EE_meta.xml"

  def getFilePath(folderName: String, directory: String, stationName: String, stationEUCode: String, prefix: String, suffix: String):String = {
    val name = stationName + "_"
    s"$folderName/$directory/$name$stationEUCode$prefix$suffix"
  }

  val dataXML = XML.loadFile(dataPath)

  case class Station(stationName: String, stationEUCode: String,
                     stationLocalCode: String, stationDescription: String,
                     stationNutsLevel0: String, stationNutsLevel1: Int,
                     stationNutsLevel2: Int, stationNutsLevel3: Int,
                     lauLevel1Code: String, lauLevel2Name: String,
                     sabeCountryCode: String, sabeUnitCode: String, sabeUnitName: String,
                     stationStartDate: String, stationLatitudeDecDegrees: Double,
                     stationLongitudeDecDegrees: Double, stationLatitudeDms: String,
                     stationLongitudeDms: String, stationAltitude: Int,
                     typeOfStation: String, stationTypeOfArea: String,
                     stationCharactOfZone: String, stationSubcRurBackg: String,
                     monitoringObj: String, meteorologicalParameter: String
                    )

  implicit val fileRW: default.ReadWriter[Station] = upickle.default.macroRW[Station]

  //FIXME if there are several meteorological parameter
  def fromXMLtoFile(node: scala.xml.Node): Station = {
    val stationInfo = node \ "station_info"
    Station(
      stationName = (stationInfo\ "station_name").text,
      stationEUCode = (node \ "station_european_code").text,
      stationLocalCode = (stationInfo \ "station_local_code").text,
      stationDescription = (stationInfo \ "station_description").text,
      stationNutsLevel0 = (stationInfo \ "station_nuts_level0").text,
      stationNutsLevel1 = (stationInfo \ "station_nuts_level1").text.toInt,
      stationNutsLevel2 = (stationInfo \ "station_nuts_level2").text.toInt,
      stationNutsLevel3 = (stationInfo \ "station_nuts_level3").text.toInt,
      lauLevel1Code = (stationInfo \ "lau_level1_code").text,
      lauLevel2Name = (stationInfo \ "lau_level2_name").text,
      sabeCountryCode = (stationInfo \ "sabe_country_code").text,
      sabeUnitCode = (stationInfo \ "sabe_unit_code").text,
      sabeUnitName = (stationInfo \ "sabe_unit_name").text,
      stationStartDate = (stationInfo \ "station_start_date").text,
      stationLatitudeDecDegrees = (stationInfo \ "station_latitude_decimal_degrees").text.toDouble,
      stationLongitudeDecDegrees = (stationInfo \ "station_longitude_decimal_degrees").text.toDouble,
      stationLatitudeDms = (stationInfo \ "station_latitude_dms").text,
      stationLongitudeDms = (stationInfo \ "station_longitude_dms").text,
      stationAltitude = (stationInfo \ "station_altitude").text.toInt,
      typeOfStation = (stationInfo \ "type_of_station").text,
      stationTypeOfArea = (stationInfo \ "station_type_of_area").text,
      stationCharactOfZone = (stationInfo \ "station_characteristic_of_zone").text,
      stationSubcRurBackg = (stationInfo \ " station_subcategory_rural_background").text,
      monitoringObj = (stationInfo \ "monitoring_obj").text,
      meteorologicalParameter = (stationInfo \ "meteorological_parameter").text
    )
  }

  val stationNodes = dataXML \ "country" \ "station"
  val stations = stationNodes.map(node => fromXMLtoFile(node))

  val destJSONFilePaths = stations.map(station => getFilePath(folderName, "json", station.stationName, station.stationEUCode, prefix = "_meta", suffix = ".json"))

  val destTSVFilePaths = stations.map(station => getFilePath(folderName, "tsv", station.stationName, station.stationEUCode, prefix = "_yearly", suffix = ".tsv"))

  for (i <- stations.indices) Utilities.saveString(upickle.default.write(stations(i), indent = 4), destJSONFilePaths(i))

  val countryJson = upickle.default.write(stations, indent = 4)
  Utilities.saveString(countryJson, folderName + "/json/stations_Estonia_meta.json" )

  case class Measurement(componentName: String, componentCaption: String, measurementUnit: String, measurementTechniquePrinciple: String
                         , year2005Mean : String, year2005Median: String)

  def getMeasurement(savedStrings: String, measurement: String): String = {
   val stringList = savedStrings.split("\n").map(_.trim).toList
   val measurementNameIndex = stringList.indexOf(measurement)
    stringList(measurementNameIndex+1)
  }

  def fromXMLtoMeasures(node: scala.xml.Node): Measurement = {
    val year2005Results = (((node \ "statistics").
      filter( _ \ "@Year" exists (_.text == "2005")) \ "statistics_average_group")
      .filter( _ \ "@value" exists (_.text == "day")) \ "statistic_set" \ "statistic_result").text
    Measurement(
      componentName = (node \ "component_name").text,
      componentCaption = (node  \"component_caption").text,
      measurementUnit = (node \"measurement_unit").text,
      measurementTechniquePrinciple = (node \"measurement_info" \ "measurement_technique_principle").text,
      year2005Mean = getMeasurement(year2005Results, "Mean"),
      year2005Median = getMeasurement(year2005Results, "P50")
    )
  }


  /** Write measurement data in TSV files
   * Creates new TSV files
   */
  def writeToTSV(stationMeasurements: Seq[Measurement], filePath: String) = {
    implicit val measurementEncoder: HeaderEncoder[Measurement] = HeaderEncoder.caseEncoder("componentName",
      "componentCaption", "measurementUnit", "measurementTechniquePrinciple"
      , "year2005Mean", "year2005Median")(Measurement.unapply)
    val csvFile = new File(filePath)
    val out = new PrintWriter(csvFile)
    val writer = out.asCsvWriter[Measurement](rfc.withHeader("componentName", "componentCaption", "measurementUnit"
      , "measurementTechniquePrinciple", "year2005Mean(P50)").withCellSeparator('\t'))
    writer.write(stationMeasurements).close()
  }


  for (i <- stations.indices) {
    val station = stationNodes.filter(_ \ "@Id" exists (_.text.contains(stations(i).stationName))) \ "measurement_configuration"
    val stationMeasurements = station.map(node => fromXMLtoMeasures(node))
    writeToTSV(stationMeasurements,destTSVFilePaths(i))
  }


}
