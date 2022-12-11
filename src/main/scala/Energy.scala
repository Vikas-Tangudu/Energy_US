import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import scala.collection.mutable.ListBuffer
import java.io.File
import org.apache.spark.sql.SQLContext
object Energy {
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

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate()

    // get the RDD
    import spark.implicits._
    def getmyrdd(filenames: ListBuffer[String]): RDD[String] = {
      val rdd_placeslist = ListBuffer[RDD[String]]()
      for (i <- 0 until filenames.size) {
        val RDD = spark.sparkContext.textFile(filenames(i))
        rdd_placeslist += RDD
      }
      rdd_placeslist.reduce(_ union _)
    }
    val energy_rdd = getmyrdd(filenames)

    //Get the Dataframe with proper Schema ***********************************************
    val energy_v1 = energy_rdd.map(mapper).toDS().withColumn("file_name", input_file_name().cast(StringType))
    val energy_v2 = energy_v1.select(
                                     col("Time"), col("month"),
                                     col("day"), col("hour"), col("building_name"),col("facility_hourly"),
                                     col("electricity"),col("gas"), col("facility_monthly"),
                                     regexp_replace(split(col("file_name"),"/")(6),"place=","").as("place")
                                     )
    // Removing Duplicate Rows ********************************
    val energy_v3 = energy_v2.dropDuplicates("Time","building_name","facility_hourly","electricity","gas","facility_monthly","place") //--- 1962204 , 36 duplicates.

    // Column wise Nulls (Nulls in our case = -1.0 ) ***********************************
//    energy_v3.createOrReplaceTempView("vw_energy")
//    val puresql = spark.sql("select " +
//      "sum(case when facility_hourly.electricity < 0 then 1 else 0 end) facility_hourly_electricity," +
//      "sum(case when facility_hourly.gas < 0 then 1 else 0 end) facility_hourly_gas," +
//      "sum(case when electricity.fans < 0 then 1 else 0 end) electricity_fans," +
//      "sum(case when electricity.cooling < 0 then 1 else 0 end) electricity_cooling," +
//      "sum(case when electricity.heating < 0 then 1 else 0 end) electricity_heating," +
//      "sum(case when electricity.interiorlights < 0 then 1 else 0 end) electricity_interiorlights," +
//      "sum(case when electricity.interiorequipment < 0 then 1 else 0 end) electricity_interiorequipment," +
//      "sum(case when gas.heating < 0 then 1 else 0 end) gas_heating," +
//      "sum(case when gas.interiorequipment < 0 then 1 else 0 end) gas_interiorequipment," +
//      "sum(case when gas.waterheater < 0 then 1 else 0 end) gas_waterheater," +
//      "sum(case when facility_monthly.electricity < 0 then 1 else 0 end) facility_monthly_electricity," +
//      "sum(case when facility_monthly.gas < 0 then 1 else 0 end) facility_monthly_gas" +
//      " from vw_energy"
//    )
//    puresql.show()

    // Partitions by month ******************************************
    energy_v3.repartition("month").rdd.getNumPartitions

  }
}
