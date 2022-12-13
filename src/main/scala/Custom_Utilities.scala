import scala.collection.mutable.ListBuffer
import java.io.File

object Custom_Utilities {

  def getallfiles(dirs: Array[File]): ListBuffer[String] = {
    val filenames: ListBuffer[String] = ListBuffer[String]()
    for (i <- 0 to dirs.length - 1) {
      if (dirs(i).isDirectory) {
        val files: Array[File] = new File(dirs(i).toString).listFiles
        for (k <- 0 to files.length - 1) {
          filenames += files(k).toString
        }
      }
    }
    filenames
  }

  case class facility_hourly(electricity: Double, gas: Double)

  case class electricity(fans: Double, cooling: Double, heating: Double, interiorlights: Double, interiorequipment: Double)

  case class gas(heating: Double, interiorequipment: Double, waterheater: Double)

  case class facility_monthly(electricity: Double, gas: Double)

  case class energy_schema(Time: String, month: Integer, day: Integer, hour: Integer, building_name: String, facility_hourly: facility_hourly, electricity: electricity, gas: gas, facility_monthly: facility_monthly)

  def mapper(line: String): energy_schema = {
    //Null values are replaced by -1: Null = -1
    val fields = line.replace("\\N", "-1.0") split (',')
    val f_hourly = fields(5).split('$')
    val ele = fields(6).split('$')
    val gs = fields(7).split('$')
    val f_monthly = fields(8).split('$')

    val F_hourly: facility_hourly = facility_hourly(f_hourly(0).toDouble, f_hourly(1).toDouble)
    val ELE: electricity = electricity(ele(0).toDouble, ele(1).toDouble, ele(2).toDouble, ele(3).toDouble, ele(4).toDouble)
    val GS: gas = gas(gs(0).toDouble, gs(1).toDouble, gs(2).toDouble)
    val F_monthly: facility_monthly = facility_monthly(f_monthly(0).toDouble, f_monthly(1).toDouble)


    val schema: energy_schema = energy_schema(fields(0), fields(1).toInt, fields(2).toInt,
      fields(3).toInt, fields(4), F_hourly, ELE, GS, F_monthly)
    schema
  }


}
