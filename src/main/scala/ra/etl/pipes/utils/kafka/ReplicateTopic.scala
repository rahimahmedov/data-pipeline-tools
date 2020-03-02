package ra.etl.pipes.utils.kafka

import java.io.File
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit

import ra.etl.pipes.utils.cli._
import com.typesafe.config.ConfigFactory
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroSerializer}
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}

import scala.collection.JavaConverters._

object ReplicateTopic  extends App {


  private final val _APPLICATION_ID = "app.id"
  private final val _INPUT_TOPIC    = "app.from.topic"
  private final val _INPUT_BROKER   = "app.from.cluster"
  private final val _INPUT_SRU      = "app.from.SRU"
  private final val _OUTPUT_TOPIC   = "app.to.topic"
  private final val _OUTPUT_BROKER  = "app.to.cluster"
  private final val _OUTPUT_SRU     = "app.to.SRU"
  private final val _FILTER_FIELD   = "app.filter.field"
  private final val _FILTER_VALUES  = "app.filter.values"

  /*
    ReplicateTopic  --configFile hoconFile.hocon
   */


  val configFilePath = args.getOrElse("--configFile", "")
  val configHocon = ConfigFactory.parseFile(new File(configFilePath))

  val filterField = configHocon.getString(_FILTER_FIELD)
  val filerValues = configHocon.getString(_FILTER_VALUES).split(",")
  val outTopic = configHocon.getString(_OUTPUT_TOPIC)

  val propsIN = new Properties()
  propsIN.put(StreamsConfig.APPLICATION_ID_CONFIG, configHocon.getString(_APPLICATION_ID))
  propsIN.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, configHocon.getString(_INPUT_BROKER))

  val propsOUT = new Properties()
  propsOUT.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configHocon.getString(_OUTPUT_BROKER))
  propsOUT.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  propsOUT.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
  propsOUT.put("schema.registry.url", configHocon.getString(_OUTPUT_SRU))
  val producerOUT = new KafkaProducer[Object, Object](propsOUT)

  // input topic's  values:
  val inputValuesSerde = new GenericAvroSerde()
  inputValuesSerde.configure(Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> configHocon.getString(_INPUT_SRU) ).asJava, false)

  val streamBuilder = new StreamsBuilder

  // get stream of input records:
  val inputStream =  streamBuilder.stream(configHocon.getString(_INPUT_TOPIC))(Consumed.`with`(Serdes.String, inputValuesSerde))

  // filter by value
  inputStream
    .filter( (_,v) => filerValues.contains(String.valueOf(v.get(filterField))) )
    .foreach( (_, v) => {
      println(s"Transferring $filterField = ${String.valueOf(v.get(filterField))}")
      val record = new ProducerRecord[AnyRef, AnyRef](outTopic, v)
      producerOUT.send(record)
    })

  // start stream
  val topology = streamBuilder.build()
  val stream = new KafkaStreams(topology, propsIN)

  println(topology.describe())


  stream.start()

  sys.ShutdownHookThread {
    stream.close(Duration.ofSeconds(10))
  }




}
