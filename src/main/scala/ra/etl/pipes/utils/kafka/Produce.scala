package ra.etl.pipes.utils.kafka

import java.net.URL
import java.util.Properties


import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader}
import org.apache.avro.io.DecoderFactory
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord

import ra.etl.pipes.utils.cli._
import ra.etl.pipes.utils.SchemaRegJSON

object Produce extends App {

  /*
     produce --broker <list of brokers> --sru <schema registry url> --topic <topic name> --key <json key> --value <json value>
   */

  val schemaRegURL = args.getOrElse("--sru", "")
  val topic = args.getOrElse("--topic", "")

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args.getOrElse("--broker", ""))
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
  props.put("schema.registry.url", schemaRegURL)
  val producer = new KafkaProducer[Object, Object](props)

  val objectMapper = new ObjectMapper() with ScalaObjectMapper
  objectMapper.registerModule(DefaultScalaModule)

  val schemaParser = new Schema.Parser()

  val keySchema = schemaParser.parse( objectMapper.readValue[SchemaRegJSON](new URL(s"${schemaRegURL}/subjects/${topic}-key/versions/latest"), classOf[SchemaRegJSON]).schema )
  val valSchema = schemaParser.parse( objectMapper.readValue[SchemaRegJSON](new URL(s"${schemaRegURL}/subjects/${topic}-value/versions/latest"), classOf[SchemaRegJSON]).schema )

  val decoderFactory = new DecoderFactory()

  val keyDecoder = decoderFactory.jsonDecoder(keySchema, args.getOrElse("--key", "{}"))
  val keyReader  = new GenericDatumReader[GenericData.Record](keySchema)
  val keyRecord = keyReader.read(null, keyDecoder)

  val valDecoder = decoderFactory.jsonDecoder(valSchema, args.getOrElse("--value", "{}"))
  val valReader  = new GenericDatumReader[GenericData.Record](valSchema)
  val valRecord = valReader.read(null, valDecoder)


  val record = new ProducerRecord[AnyRef, AnyRef](topic, keyRecord, valRecord)
  producer.send(record)
  producer.flush()
  producer.close()

}
