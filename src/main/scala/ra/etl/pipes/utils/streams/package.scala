package ra.etl.pipes.utils

import java.net.URL

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.typesafe.config.ConfigFactory
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializer}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.{DecimalType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.streaming.{Duration, Seconds}

import scala.collection.JavaConverters._

package object streams {

  private val _KAFKA_BROKER           = "app.kafka.broker"
  private val _KAFKA_SCHEMAREGURL     = "app.kafka.schemaRegURL"
  private val _KAFKA_AUTORESETOFFSET  = "app.kafka.autoResetOffest"
  private val _KAFKA_GROUPID          = "app.kafka.groupID"
  private val _SPARK_BATCHDUR         = "app.spark.batchDur"
  private val _SPARK_PART_RECCOUNT    = "app.spark.partition.maxRecordCount"
  private val _SPARK_PART_CNT         = "app.spark.partition.count"
  private val _KUDU_MASTER            = "app.kudu.master"
  private val _STREAMS                = "app.streams"
  private val _STREAMS_SRCTOPIC       = "srcTopic"
  private val _STREAMS_DESTTABLE      = "destTable"
  private val _STREAMS_TRANSFORM      = "transform"
  private val _STREAMS_FILTER         = "filter"
  private val _APP_LOGDIR             = "app.logdir"


  case class SchemaRegJSON(subject: String, version: Int, id: Int, schema: String)

  // -- utility to get schema structure -----------------------------------------------------------------------------

  def convertJSONtoStructType(jsonString: String): StructType = {
    val schema = new Schema.Parser().parse(jsonString)
    StructType(
      schema.getFields.asScala.map(
        f => StructField(f.name(), SchemaConverters.toSqlType( f.schema() ).dataType )
      )
    )
  }

  def getStructFromURL(url: URL): StructType = {
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    convertJSONtoStructType( objectMapper.readValue[SchemaRegJSON](url, classOf[SchemaRegJSON]).schema )
  }

  def getLatestValueStrucFromSchemaRegister(kschemaRegistryURL: String, topic: String): StructType =
    getStructFromURL(new URL(s"${kschemaRegistryURL}/subjects/${topic}-value/versions/latest"))

  def getLatestKeyStrucFromSchemaRegister(kschemaRegistryURL: String, topic: String): StructType =
    getStructFromURL(new URL(s"${kschemaRegistryURL}/subjects/${topic}-key/versions/latest"))

  //-----------------------------------------------------------------------------------------------------------------


  case class TransformPart(sourceTopic: String, destinationTable: String, transformSQL: String, srcStruct: StructType)

  case class SparkAppContext(kafkaParams: Map[String, Object],
                             kafkaTopics: Array[String],
                             kuduContext: KuduContext,
                             batchDur: Duration,
                             repartCnt: Int,
                             partCnt: Int,
                             transformPart: Map[String, TransformPart],
                             appLogdir: String)
  {

    // convert generic records rdd to data frame
    def toDataFrame(sqlContext: SQLContext,
                    rdd: RDD[ConsumerRecord[String, GenericRecord]],
                    srcTopic: String ): DataFrame  =
    {
      val _srcStruct = this.transformPart.get(srcTopic).get.srcStruct
      val r = rdd.filter( _.topic().equals(srcTopic)).map( v => {
        val gr = v.value()
        val _recArray = new Array[Any](_srcStruct.fields.size)
        _srcStruct.fields.foreach(f => _recArray(_srcStruct.fieldIndex(f.name)) = f.dataType match {
          case _: StringType => {
            val _val_ = gr.get(f.name)
            if (_val_ != null ) String.valueOf(gr.get(f.name)) else null
          }
          case _: DecimalType => {
            val _val_ = gr.get(f.name)
            if (_val_ != null ) BigDecimal.valueOf(gr.get(f.name).asInstanceOf[Long]) else null
          }
          case _ => gr.get(f.name)
        })
        Row.fromSeq(_recArray)
      })
      sqlContext.createDataFrame(r, _srcStruct)
    }

    // register temporary view for source data
    def createTempView(dataFrame: DataFrame, srcTopic: String): Unit =
      dataFrame.createOrReplaceTempView(s"`TMP_${srcTopic.toUpperCase}`")
  }

  object SparkAppContext {
    def apply(sc: SparkContext, filePath: String): SparkAppContext = {
      val fs = FileSystem.get(sc.hadoopConfiguration)
      val fis = fs.open(new Path(filePath))
      val confStr = new String(IOUtils.readFullyToByteArray(fis))
      fis.close()
      val conf = ConfigFactory.parseString(confStr)
      val _kafkaParams = Map[String, Object](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> conf.getString(_KAFKA_BROKER),
        ConsumerConfig.GROUP_ID_CONFIG -> conf.getString(_KAFKA_GROUPID),
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> conf.getString(_KAFKA_AUTORESETOFFSET), //autoOffsetResetConfig,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[KafkaAvroDeserializer],
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> conf.getString(_KAFKA_SCHEMAREGURL)
      )
      val streams = conf.getConfigList(_STREAMS)
      val _transformPart = streams.asScala.map(ccc => {
        val __srcTopic = ccc.getString(_STREAMS_SRCTOPIC)
        ( __srcTopic,
          TransformPart(
            __srcTopic,
            s"impala::${ccc.getString(_STREAMS_DESTTABLE)}",
            s"SELECT ${ccc.getString(_STREAMS_TRANSFORM)} FROM `TMP_${__srcTopic.toUpperCase}` WHERE ${ccc.getString(_STREAMS_FILTER)}",
            getLatestValueStrucFromSchemaRegister(conf.getString(_KAFKA_SCHEMAREGURL), __srcTopic)
          )
        )
      }).toMap
      val _kafkaTopics = streams.asScala.map( _.getString(_STREAMS_SRCTOPIC) ).toArray
      val _kuduContext = new KuduContext(conf.getString(_KUDU_MASTER), sc)
      val _batchDur = Seconds(conf.getInt(_SPARK_BATCHDUR))
      val _repartCnt = conf.getInt(_SPARK_PART_RECCOUNT)
      val _partCnt = conf.getInt(_SPARK_PART_CNT)
      val _applogdir = conf.getString(_APP_LOGDIR)
      new SparkAppContext(_kafkaParams, _kafkaTopics, _kuduContext, _batchDur, _repartCnt, _partCnt, _transformPart, _applogdir)
    }
  }

}
