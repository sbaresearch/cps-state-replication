package org.sba_research.cpsstatereplication

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Encoders.kryo
import org.apache.spark.sql._
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.sba_research.cpsstatereplication.aml.AmlStimuliParser
import org.sba_research.cpsstatereplication.aml.AmlVals.{DataTypeBool, DataTypeInt}
import org.sba_research.cpsstatereplication.kafka.KafkaStreamingClient
import org.sba_research.cpsstatereplication.model.log.RfidReaderLog
import org.sba_research.cpsstatereplication.model.network.modbus.request.ModbusReadHoldingRegistersRequest.{ModbusAduReadHoldingRegistersRequest, ModbusPduDataReadHoldingRegistersRequest}
import org.sba_research.cpsstatereplication.model.network._
import org.sba_research.cpsstatereplication.model.network.modbus.{MbTcp, ModbusAdu}
import org.sba_research.cpsstatereplication.model.network.modbus.request.{ModbusReadHoldingRegistersRequest, ModbusWriteMultipleRegistersRequest}
import org.sba_research.cpsstatereplication.model.network.modbus.request.ModbusWriteMultipleRegistersRequest.{ModbusAduWriteMultipleRegistersRequest, ModbusPduDataWriteMultipleRegistersRequest}
import org.sba_research.cpsstatereplication.model.network.mqtt.MqttPublish
import org.sba_research.cpsstatereplication.model.node.ProgramNode
import org.sba_research.cpsstatereplication.model.stimulus.{RfidStimulus, TagStimulus}
import org.sba_research.cpsstatereplication.util.KafkaSparkApp

import scala.language.implicitConversions
import scala.util.Try
import scala.util.parsing.json.JSONObject

object CpsStateReplicationApp extends KafkaSparkApp with App with LazyLogging {

  val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  import sparkSession.implicits._
  import scala.collection.JavaConversions._
  import org.apache.spark.sql.functions._

  val streamingContext = new StreamingContext(sparkContext, Seconds(streamingBatchDuration.toSeconds))

  val pTopics = appConfig.getStringList("kafka.p_topics").toSeq.toArray
  val vTopics = appConfig.getStringList("kafka.v_topics").toSeq.toArray
  val consumeTopics = pTopics ++ vTopics
  val eventStream = KafkaStreamingClient.createStream[String, String](streamingContext, kafkaConsumerConfig, consumeTopics)
  val stimuliTopic = appConfig.getString("kafka.stimuli_topic")
  val kafkaProducer = KafkaStreamingClient.createProducer(streamingContext.sparkContext, kafkaProducerConfig)

  val amlStimuli = AmlStimuliParser.parseFromFile(appConfig.getString("aml.path"))
  /* Broadcasting AML Stimuli map across nodes. */
  val broadcastedAmlStimuli = streamingContext.sparkContext.broadcast(amlStimuli)

  /* Create type alias for simplicity. */
  type Record = ConsumerRecord[String, String]

  def process(record: (RDD[Record], Time)): Unit = record match {
    case (rdd, /* time */ _) if !rdd.isEmpty =>

      def isTopic(x: Record, t: String) = x.topic == t

      def tableExists(tmpTableName: String) = sparkSession.catalog.tableExists(tmpTableName)

      def getDataFrame(rdd: RDD[Record]) = {
        val ds = sparkSession.createDataset(rdd.map(_.value))
        /* Read JSON and infer schema. */
        sparkSession.read. /*schema(schema).*/ json(ds)
      }

      def mergeDfWithTmpTable(df: DataFrame, tmpTableName: String): Unit = {
        if (!df.rdd.isEmpty()) {
          if (!tableExists(tmpTableName)) df.createOrReplaceTempView(tmpTableName)
          else {
            val existingDfMqttPublishRequests = sparkSession.sql(s"SELECT * FROM $tmpTableName")
            existingDfMqttPublishRequests.union(df).createOrReplaceTempView(tmpTableName)
          }
        }
      }

      def processTrafficStimuli(rdd: RDD[Record]): Unit = {

        def getDfModbusRequests(df: DataFrame) = {

          def isModbusRequest: Column =
            $"layers.tcp.tcp_tcp_srcport" =!= MbTcp.port && $"layers.tcp.tcp_tcp_dstport" === MbTcp.port

          df
            .filter($"layers.modbus".isNotNull && isModbusRequest)
            .select(
              $"timestamp" as "timestamp",
              $"layers.ip.ip_ip_src" as "ip.src",
              $"layers.ip.ip_ip_dst" as "ip.dst",
              $"layers.tcp.tcp_tcp_srcport" as "tcp.srcport",
              $"layers.tcp.tcp_tcp_dstport" as "tcp.dstport",
              $"layers.mbtcp.*",
              $"layers.modbus.*"
            )

        }

        def getDsModbusRequests(df: DataFrame): Dataset[Packet] = {

          implicit val packetEncoder: Encoder[Packet] = kryo[Packet]

          def getPacket(row: Row) = {
            val timestamp = row.getAs[String]("timestamp")

            val commonLayers = List(
              Ip(
                src = row.getAs[String]("ip.src"),
                dst = row.getAs[String]("ip.dst")
              ),
              Tcp(
                srcPort = row.getAs[String]("tcp.srcport").toInt,
                dstPort = row.getAs[String]("tcp.dstport").toInt
              ),
              MbTcp(
                transId = row.getAs[String]("mbtcp_mbtcp_trans_id").toInt,
                protId = row.getAs[String]("mbtcp_mbtcp_prot_id").toInt,
                len = row.getAs[String]("mbtcp_mbtcp_len").toInt,
                unitId = row.getAs[String]("mbtcp_mbtcp_unit_id").toInt
              )
            )

            val functionCode = row.getAs[String]("modbus_modbus_func_code").toInt.toByte
            val modbus: Option[Layer] = functionCode match {
              case ModbusWriteMultipleRegistersRequest.functionCode => Some(
                ModbusWriteMultipleRegistersRequest(
                  startingAddress = BigInt(row.getAs[String]("modbus_modbus_reference_num").toInt).toByteArray,
                  quantityOfRegisters = BigInt(row.getAs[String]("modbus_modbus_word_cnt").toInt).toByteArray,
                  registersValue = BigInt(row.getAs[String]("text_modbus_regval_uint16").toInt).toByteArray
                )
              )
              case ModbusReadHoldingRegistersRequest.functionCode => Some(
                ModbusReadHoldingRegistersRequest(
                  startingAddress = BigInt(row.getAs[String]("modbus_modbus_reference_num").toInt).toByteArray,
                  quantityOfRegisters = BigInt(row.getAs[String]("modbus_modbus_word_cnt").toInt).toByteArray
                )
              )
              case _ => None
            }
            Packet(timestamp, commonLayers ++ modbus)
          }
          /* getPacket method is wrapped around a Try block, because if some fields do not exists
           * (e.g., if tshark < 2.4.6 Modbus dissector changed) an IllegalArgumentException will be thrown.
           */
          df.flatMap(row => Try(getPacket(row)).toOption)
        }

        val df = getDataFrame(rdd)
        /* Check if Modbus layer exists. */
        if (Try(df("layers.modbus")).isSuccess) {
          val dfModbusRequests = getDfModbusRequests(df)
          val dsModbusRequests = getDsModbusRequests(dfModbusRequests)
          // dfModbusRequests.show()
          dsModbusRequests.foreachPartition { partitionOfRecords =>

            // Cf. https://stackoverflow.com/a/39442829/5107545
            type TagStimulusEncoded = (String, String, String, Option[String])

            implicit def stimulusToEncoded(s: TagStimulus): TagStimulusEncoded = (s.timestamp, s.twinName, s.tagName, s.value)

            implicit def stimulusFromEncoded(e: TagStimulusEncoded): TagStimulus = TagStimulus(e._1, e._2, e._3, e._4)

            implicit val layerEncoder: Encoder[Layer] = kryo[Layer]

            /* Retrieve tuple consisting of timestamp and Modbus layer. */
            val itModbusLayer: Iterator[(String, Layer)] =
              partitionOfRecords
                .flatMap { p =>
                  for {
                    l <- p.layers.find {
                      case _: ModbusAdu => true
                      case _ => false
                    }
                  } yield (p.timestamp, l)
                }

            /**
              * Used to retrieve a sequence of stimulus from a single Modbus request.
              *
              * @param timestamp the timestamp of the stimulus
              * @param layer     the Modbus request layer
              * @return a sequence of stimulus
              */
            def getStimuliFromModbusRequest(timestamp: String, layer: Layer): Option[Seq[TagStimulus]] = {

              /**
                * Looks up Modbus request from Map to get sequence of stimulus.
                *
                * @param mbReq    the complete Modbus request, including the registers values
                * @param mapMbReq the temporary Modbus request used to lookup stimulus in Map (if Modbus request type does not include a registers value field, this variable is none)
                * @return optionally a sequence of stimulus
                */
              def lookup(mbReq: ModbusAdu, mapMbReq: Option[ModbusAdu] = None): Option[Seq[TagStimulus]] =
                broadcastedAmlStimuli.value.network.get(mapMbReq.getOrElse(mbReq)).map { a =>
                  a.flatMap { link =>
                    val twinName = link.src.node.name
                    val tagName = link.src.tag
                    /* Parse value from packet. */
                    mbReq.pdu.data.map {
                      case mb0x10Data: ModbusPduDataWriteMultipleRegistersRequest => Some(mb0x10Data.registersValue)
                      case _: ModbusPduDataReadHoldingRegistersRequest => None
                      case _ => None
                    }
                      /* Create Stimulus based on optional registers value. */
                      .flatMap {
                      case Some(regVal) =>
                        /* Match data type. */
                        val dataType = link.src.node match {
                          case n: ProgramNode => n.program.tags.get(tagName)
                          case _ => None
                        }
                        /* Stabilize match clauses.  */
                        val dataTypeInt = DataTypeInt.toString
                        val dataTypeBool = DataTypeBool.toString
                        /* Transform registers value to proper data type. */
                        val value = dataType.flatMap {
                          case `dataTypeInt` =>
                            Some(BigInt(regVal.toArray).intValue.toString)
                          case `dataTypeBool` =>
                            val intVal = BigInt(regVal.toArray).intValue
                            val res = if (intVal == 1) "True" else "False"
                            Some(res)
                          case _ => None
                        }
                        /* Return stimulus (set var). */
                        Some(TagStimulus(timestamp, twinName, tagName, value))
                      case None =>
                        /* Return stimulus (get var). */
                        Some(TagStimulus(timestamp, twinName, tagName, None))
                    }
                  }
                }

              layer match {
                case mb0x10: ModbusAduWriteMultipleRegistersRequest =>
                  /* Copy Modbus packet with empty registersValue, a prerequisite for the lookup. */
                  val tmpPduData = mb0x10.pdu.data.map(_.copy(registersValue = ModbusWriteMultipleRegistersRequest.defaultRegistersValue))
                  val tmpMbRq = mb0x10.copy(pdu = mb0x10.pdu.copy(data = tmpPduData))
                  /* Perform lookup. */
                  lookup(mb0x10, Some(tmpMbRq))
                case mb0x03: ModbusAduReadHoldingRegistersRequest => lookup(mb0x03)
                case _ => None
              }

            }

            /* Retrieve stimuli by lookup in Map. */
            val itStimuli: Iterator[TagStimulus] = itModbusLayer
              .flatMap(x => getStimuliFromModbusRequest(x._1, x._2))
              /* Flatten rows. */
              .flatten

            /* Sending stimuli to Kafka. */
            itStimuli.foreach { message =>
              /* JSON Marshalling. */
              val baseStimulusJson = Map(
                "timestamp" -> message.timestamp,
                "twin_name" -> message.twinName,
                "tag_name" -> message.tagName
              )
              /* If value is not none, we are processing a 'set' stimulus. */
              val setStimulusJson = message.value.map(value => baseStimulusJson + ("value" -> value))
              /* Resolve final stimulus. */
              val stimulusJson = setStimulusJson.getOrElse(baseStimulusJson)
              kafkaProducer.value.send(stimuliTopic, JSONObject(stimulusJson).toString)
            }

          }
        }

      }

      def processLogsStimuli(rdd: RDD[Record]): Unit = {

        def getDsRfidReaderLogs(df: DataFrame): Dataset[RfidReaderLog] = {
          implicit val packetEncoder: Encoder[RfidReaderLog] = kryo[RfidReaderLog]

          df.map { row =>
            val timestamp = row.getAs[String]("timestamp")
            val name = row.getAs[String]("name")
            val candy = row.getAs[String]("candy")
            RfidReaderLog(name, timestamp, candy)
          }
        }

        val dfRfidReaderLogs = getDataFrame(rdd)
        // dfRfidReaderLogs.show()
        val dsRfidReaderLogs = getDsRfidReaderLogs(dfRfidReaderLogs)
        dsRfidReaderLogs.foreachPartition { partitionOfRecords =>

          // Cf. https://stackoverflow.com/a/39442829/5107545
          type RfidStimulusEncoded = (String, String, String)

          implicit def stimulusToEncoded(s: RfidStimulus): RfidStimulusEncoded = (s.timestamp, s.twinName, s.value)

          implicit def stimulusFromEncoded(e: RfidStimulusEncoded): RfidStimulus = RfidStimulus(e._1, e._2, e._3)

          def getStimulusFromRfidReaderLog(log: RfidReaderLog): Option[RfidStimulus] = {

            def lookup(lookupLog: RfidReaderLog): Option[RfidStimulus] =
              Option(broadcastedAmlStimuli.value.log.contains(lookupLog))
                .collect { case true => RfidStimulus(log.timestamp, log.name, log.value) }

            /* Copy instance and blank properties that are irrelevant for lookup & perform said lookup. */
            lookup(log.copy(timestamp = "", value = ""))
          }

          /* Retrieve stimuli by lookup in Map. */
          val itStimuli: Iterator[RfidStimulus] = partitionOfRecords
            .flatMap(getStimulusFromRfidReaderLog)
          /* Sending stimuli to Kafka. */
          itStimuli.foreach { message =>
            /* JSON Marshalling. */
            val stimulusJson = Map(
              "timestamp" -> message.timestamp,
              "twin_name" -> message.twinName,
              "value" -> message.value
            )
            kafkaProducer.value.send(stimuliTopic, JSONObject(stimulusJson).toString)
          }
        }
      }

      def processTrafficComparison(rddP: RDD[Record], rddV: RDD[Record]): Unit = {

        val pTmpTableName = "p_mqtt"
        val vTmpTableName = "v_mqtt"

        def isMqttRequest: Column =
          $"layers.tcp.tcp_tcp_srcport" =!= 1883 && $"layers.tcp.tcp_tcp_dstport" === 1883

        def isMqttPublish: Column =
          $"layers.mqtt.mqtt_hdrflags_mqtt_msgtype" === MqttPublish.`type`.toString

        def getDfMqttPublishRequests(df: DataFrame): DataFrame = {

          def selectBaseCols(df: DataFrame): DataFrame =
            df
              .filter($"layers.mqtt".isNotNull && isMqttRequest && isMqttPublish)
              .select(
                $"timestamp" as "timestamp",
                $"layers.eth.eth_eth_src" as "eth.src",
                $"layers.eth.eth_eth_dst" as "eth.dst",
                $"layers.ip.ip_ip_src" as "ip.src",
                $"layers.ip.ip_ip_dst" as "ip.dst",
                $"layers.tcp.tcp_tcp_srcport" as "tcp.srcport",
                $"layers.tcp.tcp_tcp_dstport" as "tcp.dstport",
                $"layers.mqtt.*"
              )

          def selectMqttTshark226(df: DataFrame): DataFrame =
            df.select(
              $"timestamp",
              $"`eth.src`",
              $"`eth.dst`",
              $"`ip.src`",
              $"`ip.dst`",
              $"`tcp.srcport`",
              $"`tcp.dstport`",
              $"mqtt_hdrflags_mqtt_msgtype" as "mqtt.msgtype",
              $"mqtt_hdrflags_mqtt_dupflag" as "mqtt.dupflag",
              $"mqtt_hdrflags_mqtt_qos" as "mqtt.qos",
              $"mqtt_hdrflags_mqtt_retain" as "mqtt.retain",
              $"text_mqtt_len" as "mqtt.len",
              $"text_mqtt_topic" as "mqtt.topic",
              $"text_mqtt_msg" as "mqtt.msg"
            )

          def selectMqttTshark246(df: DataFrame): DataFrame =
            df.select(
              $"timestamp",
              $"`eth.src`",
              $"`eth.dst`",
              $"`ip.src`",
              $"`ip.dst`",
              $"`tcp.srcport`",
              $"`tcp.dstport`",
              $"mqtt_hdrflags_mqtt_msgtype" as "mqtt.msgtype",
              $"mqtt_hdrflags_mqtt_dupflag" as "mqtt.dupflag",
              $"mqtt_hdrflags_mqtt_qos" as "mqtt.qos",
              $"mqtt_hdrflags_mqtt_retain" as "mqtt.retain",
              $"mqtt_mqtt_len" as "mqtt.len",
              $"mqtt_mqtt_topic" as "mqtt.topic",
              $"mqtt_mqtt_msg" as "mqtt.msg"
            )

          val baseDf = selectBaseCols(df)
          /* If DataFrame is empty, return empty base Data Frame (will be discarded afterwards). */
          if (baseDf.rdd.isEmpty()) baseDf
          /* Otherwise, try to map columns. MQTT fileds differ between tshark 2.4.6 and 2.2.6. */
          else Try(selectMqttTshark246(baseDf)).getOrElse(Try(selectMqttTshark226(baseDf)).getOrElse(baseDf))
        }

        def containsMqttPublishRequest(df: DataFrame): Boolean = {
          /* Check if MQTT layer exists. */
          if (Try(df("layers.mqtt")).isSuccess) {
            val mqttPublishDf = df.filter($"layers.mqtt".isNotNull && isMqttRequest && isMqttPublish)
            !mqttPublishDf.rdd.isEmpty()
          } else false
        }

        val dfP = getDataFrame(rddP)
        val dfV = getDataFrame(rddV)

        val containsPubReqP = containsMqttPublishRequest(dfP)
        val containsPubReqV = containsMqttPublishRequest(dfV)

        if (containsPubReqP) mergeDfWithTmpTable(getDfMqttPublishRequests(dfP), pTmpTableName)
        if (containsPubReqV) mergeDfWithTmpTable(getDfMqttPublishRequests(dfV), vTmpTableName)

        if ((containsPubReqP || containsPubReqV) && (tableExists(pTmpTableName) && tableExists(vTmpTableName))) {
          val pMqtt = sparkSession.sql(s"SELECT * FROM $pTmpTableName").orderBy(asc("timestamp"))
          val vMqtt = sparkSession.sql(s"SELECT * FROM $vTmpTableName").orderBy(asc("timestamp"))
          //pMqtt.show()
          //vMqtt.show()
          val ignoredCols = Seq("timestamp", "tcp.srcport", "tcp.dstport")
          val cols = pMqtt.columns.toSeq.filter(n => !ignoredCols.contains(n))
          val diff = pMqtt.join(vMqtt, cols, "leftanti")
          if (!diff.rdd.isEmpty()) {
            val pCount = pMqtt.count()
            val vCount = vMqtt.count()
            logger.info(s"Count [pMQTT=$pCount,vMQTT=$vCount].")
            diff.show()
            /* Reset temp tables if both tables have the same amount of rows. */
            if (pCount == vCount) {
              logger.info(s"Dropping both temporary views $pTmpTableName and $vTmpTableName.")
              sparkSession.catalog.dropTempView(pTmpTableName)
              sparkSession.catalog.dropTempView(vTmpTableName)
            }
          } else logger.info("No differences found between virtual and physical environment.")
        }

      }

      def processLogsComparison(rddP: RDD[Record], rddV: RDD[Record]): Unit = {
        /* Only check CandySensor1 results. */
        // TODO: Get this from specification
        val devName = "CandySensor1"

        val pTmpTableName = "p_candy"
        val vTmpTableName = "v_candy"

        def containsDeviceLogs(df: DataFrame, name: String): Boolean = {
          /* Check if name field in log of real device or digital twin exists. */
          if (Try(df("name")).isSuccess) {
            val devLogs = df.filter($"name".isNotNull && $"name" === name)
            !devLogs.rdd.isEmpty()
          } else false
        }

        def getDfCandySensorLogs(df: DataFrame, name: String): DataFrame =
          df
            .filter($"name".isNotNull && $"name" === name)
            .select($"timestamp", $"candy")


        val dfP = getDataFrame(rddP)
        val dfV = getDataFrame(rddV)

        val containsCandySensorLogsP = containsDeviceLogs(dfP, devName)
        val containsCandySensorLogsV = containsDeviceLogs(dfV, devName)

        if (containsCandySensorLogsP) mergeDfWithTmpTable(getDfCandySensorLogs(dfP, devName), pTmpTableName)
        if (containsCandySensorLogsV) mergeDfWithTmpTable(getDfCandySensorLogs(dfV, devName), vTmpTableName)

        if ((containsCandySensorLogsP || containsCandySensorLogsV) && (tableExists(pTmpTableName) && tableExists(vTmpTableName))) {
          val pCandy = sparkSession.sql(s"SELECT * FROM $pTmpTableName").orderBy(asc("timestamp"))
          val vCandy = sparkSession.sql(s"SELECT * FROM $vTmpTableName").orderBy(asc("timestamp"))
          val ignoredCols = Seq("timestamp")
          val cols = pCandy.columns.toSeq.filter(n => !ignoredCols.contains(n))
          val diff = pCandy.join(vCandy, cols, "leftanti")
          if (!diff.rdd.isEmpty()) {
            val pCount = pCandy.count()
            val vCount = vCandy.count()
            logger.info(s"Count [pCandy=$pCount,vCandy=$vCount].")
            diff.show()
            /* Reset temp tables if both tables have the same amount of rows. */
            if (pCount == vCount) {
              logger.info(s"Dropping both temporary views $pTmpTableName and $vTmpTableName.")
              sparkSession.catalog.dropTempView(pTmpTableName)
              sparkSession.catalog.dropTempView(vTmpTableName)
            }
          } else logger.info("No differences found between virtual and physical environment.")
        }

      }

      /* Split RDDs by topic. */
      val rddPTraffic = rdd.filter(isTopic(_, consumeTopics(0)))
      val rddPLogs = rdd.filter(isTopic(_, consumeTopics(1)))
      val rddVTraffic = rdd.filter(isTopic(_, consumeTopics(2)))
      val rddVLogs = rdd.filter(isTopic(_, consumeTopics(3)))

      processTrafficStimuli(rddPTraffic)
      processLogsStimuli(rddPLogs)
      processTrafficComparison(rddPTraffic, rddVTraffic)
      processLogsComparison(rddPLogs, rddVLogs)

    case _ =>
  }

  eventStream.foreachRDD((x, y) => process((x, y)))
  /* Start the execution of streams. */
  streamingContext.start()
  /* Wait for kill signals. */
  streamingContext.awaitTermination()

}
