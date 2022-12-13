import Custom_Utilities._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import scala.collection.mutable.ListBuffer
import java.io.File

object Energy {
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
                                     col("electricity"),col("gas"), col("facility_monthly"), col("file_name"),
                                     regexp_replace(split(col("file_name"),"/")(6),"place=","").as("place")
                                     )
    // Removing Duplicate Rows ********************************
    val energy_v3 = energy_v2.dropDuplicates("Time","building_name","facility_hourly","electricity","gas","facility_monthly","place") //--- 1962204 , 36 duplicates.

    // Column wise Nulls (Nulls in our case = -1.0 ) ***********************************
    //energy_v3.createOrReplaceTempView("vw_energy")
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
   // val puresql = spark.sql("select distinct(month) from vw_energy")
   // puresql.show()

    // Partitions by month ******************************************
    println("Before partition : " + energy_v3.rdd.getNumPartitions)
    energy_v3.repartition(12,$"month").rdd.getNumPartitions
    println("After partition : " + energy_v3.rdd.getNumPartitions)
    val partition_wise_counts = energy_v3.withColumn("partition_ID", spark_partition_id())
      .groupBy("partition_ID")
      .count()

    println("per-partition counts of records")
    partition_wise_counts.show()
  }
}
