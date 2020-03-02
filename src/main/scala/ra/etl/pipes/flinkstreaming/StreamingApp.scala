package ra.etl.pipes.flinkstreaming

import java.net.URL
import java.util.Properties

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import ra.etl.pipes.utils.streams.SchemaRegJSON
import ra.etl.pipes.utils.cli._

import org.apache.flink.streaming.api.scala._

object StreamingApp extends App {

  /*
    usage:
    streamingapp --broker <address> --sru <schema registry URL> --group-id <group id> --topics <list of topics with "," separated>
   */

  val topic = args.getOrElse("--topic", "")
  val schemaRegURL = args.getOrElse("--sru", "")

  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, args.getOrElse("--broker", ""))
  props.put(ConsumerConfig.GROUP_ID_CONFIG, args.getOrElse("--group-id", ""))
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer])
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer])
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  props.put("schema.registry.url", schemaRegURL)

  val objectMapper = new ObjectMapper() with ScalaObjectMapper
  objectMapper.registerModule(DefaultScalaModule)
  val schemaParser = new Schema.Parser()
  val valSchema = schemaParser.parse( objectMapper.readValue[SchemaRegJSON](new URL(s"${schemaRegURL}/subjects/${topic}-value/versions/latest"), classOf[SchemaRegJSON]).schema )

  // initialize flink env:
  val flinkEnv = StreamExecutionEnvironment.getExecutionEnvironment
  flinkEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val stream = flinkEnv.addSource(
    new FlinkKafkaConsumer[GenericRecord](
      topic,
      ConfluentRegistryAvroDeserializationSchema.forGeneric(valSchema, schemaRegURL),
      props
    )
  ).print()

  // execute
  flinkEnv.execute()

}
