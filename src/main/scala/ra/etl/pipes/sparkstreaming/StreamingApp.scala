package ra.etl.pipes.sparkstreaming

import java.util.Calendar

import org.apache.avro.generic.GenericRecord
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import ra.etl.pipes.utils.streams.SparkAppContext
import ra.etl.pipes.utils.cli._

import scala.util.{Failure, Success, Try}

object StreamingApp extends App {

  @transient lazy val logger = LogManager.getLogger(getClass)

  val configFilePath = args.getOrElse("--configFile", "")

  // Spark Streaming related config
  val sparkConf = new SparkConf().setAppName(args.getOrElse("--appName",  "Spark_Streaming_Generic_From_Kafka_To_Kudu"))
  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  val sparkContext = sparkSession.sparkContext
  val sqlContext = sparkSession.sqlContext

  // initialize application and streaming context:
  val appContext = SparkAppContext(sparkContext, configFilePath)
  val streamingContext = new StreamingContext(sparkContext, appContext.batchDur)

  // initialize kafka streaming:
  val kafkaStream = KafkaUtils.createDirectStream[String, GenericRecord](
    streamingContext, PreferConsistent,
    Subscribe[String, GenericRecord](appContext.kafkaTopics, appContext.kafkaParams))

  // transform and load RDDs
  kafkaStream.foreachRDD(rdd => {

    val rddCount = rdd.count()
    val repartCnt = appContext.repartCnt
    val partCnt = appContext.partCnt

    // repartition RDD is necessary :
    val insertRDD = { if(rddCount/partCnt > repartCnt) rdd.repartition({rddCount/repartCnt}.asInstanceOf[Int]) else rdd }

    //foreach topic separate transformation and destination tables:
    appContext.kafkaTopics.foreach( kafkaTopic => {
      val transform = appContext.transformPart.get(kafkaTopic).get
      val df = appContext.toDataFrame(sqlContext, insertRDD, kafkaTopic)
      appContext.createTempView(df, kafkaTopic)
      val targetDf = sqlContext.sql(transform.transformSQL)
      Try(appContext.kuduContext.upsertRows(targetDf,transform.destinationTable))
      match {
        case Success(_) =>
        case Failure(e) => {
          logger.warn(s"Could not insert records -  ${targetDf.count()} recs! Exception:\n\t\t${e.getMessage}")
          targetDf.write.format("avro")
            .save(s"${appContext.appLogdir}/${kafkaTopic}-${Calendar.getInstance().getTime.getTime}")
        }
      }
    })

  })

  streamingContext.start()
  streamingContext.awaitTermination()

}
