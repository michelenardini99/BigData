package flights

import org.apache.spark.sql.SparkSession
import utils._
import org.apache.spark.sql.SaveMode
import org.apache.spark.{HashPartitioner, SparkConf}

object FlightsAnalysis {

  val path_to_dataset = "datasets/"

  val path_flights = path_to_dataset + "flights_final.csv"
  val path_airports = path_to_dataset + "airports.csv"

  val path_output_not_opt = "output/avgDelayNotOpt"
  val path_output_opt = "output/avgDelayOpt"

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("Flights job")
    //      .set("spark.executor.memory", "4g")
    //      .set("spark.executor.cores", "4")
    //      .set("spark.executor.instances", "2")
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()

    val sqlContext = spark.sqlContext // needed to save as CSV
    import sqlContext.implicits._


    if(args.length < 2){
      println("The first parameter should indicate the deployment mode (\"local\" or \"remote\")")
      println("The second parameter should indicate the job: "
        + "1 for AvgDelay Not Optimized (join-and-agg), "
        + "2 for AvgDelay Optimized (bjoin-and-agg), ")
      return
    }

    val deploymentMode = args(0)
    var writeMode = deploymentMode

    val job = args(1)

    val columnNames = Seq(
      "Airline", "City", "State",
      "Avg_Arrival_Delay", "Avg_Airline_Delay", "Avg_Weather_Delay",
      "Avg_NAS_Delay", "Avg_Security_Delay", "Avg_Late_Aircraft_Delay"
    )

    val airports = spark.read
      .option("header", "true")
      .csv(Commons.getDatasetPath(deploymentMode, path_airports))
      .rdd
      .flatMap(FlightsParser.parseAirports)

    val flights = spark.read
      .option("header", "true")
      .csv(Commons.getDatasetPath(deploymentMode, path_flights))
      .rdd
      .flatMap(FlightsParser.parseFlights)

    val airportsRdd = airports
      .map(x => (x._1, (x._2, x._3, x._4)))
      .reduceByKey((x, _) => x)

    if(job == "1"){
      val nonOptJoin = flights
        .filter(_._5 > 0) // Synchronized with Job 2: analyzes only flights with positive delay
        .map(x => (x._1, x))
        .join(airportsRdd)
        .map({ case (_, (f, a)) =>
          ((f._2, a._2, a._3), (f._5, f._6, f._7, f._8, f._9, f._10, 1))
        })
        .reduceByKey({ (x, y) =>
          (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6, x._7 + y._7)
        })
        .mapValues({ v =>
          (
            v._1.toDouble / v._7,
            v._2.toDouble / v._7,
            v._3.toDouble / v._7,
            v._4.toDouble / v._7,
            v._5.toDouble / v._7,
            v._6.toDouble / v._7
          )
        })
        .map({ case ((airline, city, state), v) =>
          (airline, city, state, v._1, v._2, v._3, v._4, v._5, v._6)
        })
        .coalesce(1)
        .toDF(columnNames: _*)
        .write
        .format("csv")
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .save(Commons.getDatasetPath(writeMode,path_output_not_opt))

    }else if(job == "2"){
      val broadcastAirports = spark.sparkContext.broadcast(airportsRdd.collectAsMap()) // collectAsMap implicitly handles duplicates by taking the last one

      val optJob = flights
        .filter(_._5 > 0)
        .flatMap({ f =>
          broadcastAirports.value.get(f._1).map({ a =>
            ((f._2, a._2, a._3), (f._5, f._6, f._7, f._8, f._9, f._10, 1))
          })
        })
        .reduceByKey({ (x, y) =>
          (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6, x._7 + y._7)
        })
        .mapValues({ v =>
          (
            v._1 / v._7, v._2 / v._7, v._3 / v._7,
            v._4 / v._7, v._5 / v._7, v._6 / v._7
          )
        })
        .map({ case ((airline, city, state), v) =>
          (airline, city, state, v._1, v._2, v._3, v._4, v._5, v._6)
        })
        .coalesce(1)
        .toDF(columnNames: _*)
        .write
        .format("csv")
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .save(Commons.getDatasetPath(writeMode,path_output_opt))
    }else{
      println("Wrong Job Number")
    }
  }

}