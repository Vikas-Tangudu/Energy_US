import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.types.StringType
import java.util._
import scala.collection.mutable.ListBuffer
import java.io.File
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.functions.to_timestamp
import org.json4s.scalap.scalasig.ClassFileParser.fields
import org.apache.spark.sql.functions.unix_timestamp
import java.sql.Timestamp
object Rough {
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
    return filenames
  }

  case class facility_hourly(electricity: Double, gas: Double)
  case class electricity(fans: Double, cooling: Double, heating: Double, interiorlights: Double, interiorequipment: Double)
  case class gas(heating: Double, interiorequipment: Double, waterheater: Double)
  case class facility_monthly(electricity: Double, gas: Double)
  case class energy_schema(Time: String, month: Integer, day: Integer, hour: Integer, building_name: String, facility_hourly: facility_hourly, electricity: electricity, gas: gas, facility_monthly: facility_monthly)

  def mapper(line: String): energy_schema = {
    //Null values are replaced by -1: Null = -1
    val fields = line.replace("\\N","-1.0")split(',')
    val f_hourly = fields(5).split('$')
    val ele = fields(6).split('$')
    val gs = fields(7).split('$')
    val f_monthly = fields(8).split('$')

    val F_hourly:facility_hourly  = facility_hourly(f_hourly(0).toDouble,f_hourly(1).toDouble)
    val ELE : electricity = electricity(ele(0).toDouble,ele(1).toDouble,ele(2).toDouble,ele(3).toDouble,ele(4).toDouble)
    val GS : gas = gas(gs(0).toDouble,gs(1).toDouble,gs(2).toDouble)
    val F_monthly:facility_monthly  = facility_monthly(f_monthly(0).toDouble,f_monthly(1).toDouble)


    val schema: energy_schema = energy_schema(fields(0), fields(1).toInt, fields(2).toInt,
      fields(3).toInt,fields(4),F_hourly,ELE,GS,F_monthly)
    schema
  }

  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // get all the file names in the directory
    val filenames = getallfiles(new File("C:\\Users\\91918\\Desktop\\prod_energy_V2").listFiles)
    //    //for (i <- 0 to filenames.length - 1) {
    //      println(filenames(i))
    //    }
    val spark = SparkSession
      .builder
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate()

    // Create a SparkSession using every core of the local machine
    import spark.implicits._
    val input = spark.sparkContext.textFile(filenames(0))
    val energy = input.map(mapper).toDS().withColumn("file_name", input_file_name().cast(StringType))
    println(energy.count())
    energy.show()

    //    for (i <- 1 to filenames.length-1) {
    //      val input = spark.sparkContext.textFile(filenames(i))
    //      val tempdf = input.map(mapper).toDS().withColumn("file_name", input_file_name().cast(StringType))
    //      energy.unionAll(tempdf)
    //    }
    val input2 = spark.sparkContext.textFile(filenames(1))
    println(input2.count())
    val tempdf = input2.map(mapper).toDS().withColumn("file_name", input_file_name().cast(StringType))
    tempdf.show()
    energy.union(tempdf)

    println(energy.count())
    println(input.getClass)
    println(energy.getClass)



    println("*****************************")

    def getmyrdd(inputpath: String): RDD[String] = {
      val RDD = spark.sparkContext.textFile(inputpath)
      RDD
    }

    val placeslist = ListBuffer[RDD[String]]()
    for (i <- 0 until filenames.size) {
      placeslist += getmyrdd(filenames(i))
    }
    val energy_rdd = placeslist.reduce(_ union _)
    println(energy_rdd.count())
    val energy2 = energy_rdd.map(mapper).toDS().withColumn("file_name", input_file_name().cast(StringType))
    println(energy_rdd.getClass)
    println(energy2.getClass)
    //)
//    val energy_v3 = energy_v2.dropDuplicates("Time", "building_name", "facility_hourly", "electricity", "gas", "facility_monthly", "place") //--- 1962204 , 36 duplicates.
//
//    energy_v3.createOrReplaceTempView("vw_energy")
//    val puresql = spark.sql("select \nsum(case when facility_hourly.electricity < 0 then 1 else 0 end) facility_hourly_electricity,\nsum(case when facility_hourly.gas < 0 then 1 else 0 end) facility_hourly_gas,\nsum(case when electricity.fans < 0 then 1 else 0 end) electricity_fans,\nsum(case when electricity.cooling < 0 then 1 else 0 end) electricity_cooling,\nsum(case when electricity.heating < 0 then 1 else 0 end) electricity_heating,\nsum(case when electricity.interiorlights < 0 then 1 else 0 end) electricity_interiorlights,\nsum(case when electricity.interiorequipment < 0 then 1 else 0 end) electricity_interiorequipment,\nsum(case when gas.heating < 0 then 1 else 0 end) gas_heating,\nsum(case when gas.interiorequipment < 0 then 1 else 0 end) gas_interiorequipment,\nsum(case when gas.waterheater < 0 then 1 else 0 end) gas_waterheater,\nsum(case when facility_monthly.electricity < 0 then 1 else 0 end) facility_monthly_electricity,\nsum(case when facility_monthly.gas < 0 then 1 else 0 end) facility_monthly_gas,\nfrom vw_energy")
//    puresql.show()

  }
}
