package org.sba_research.cpsstatereplication.aml

import java.io.File

import com.typesafe.config.ConfigFactory
import org.sba_research.cpsstatereplication.aml.AmlVals._
import org.sba_research.cpsstatereplication.model.link.{InternalLink, NetworkStimulusLink, NodeStimulusLink}
import org.sba_research.cpsstatereplication.model.log.{RfidReaderLog, SystemLog}
import org.sba_research.cpsstatereplication.model.network.modbus.ModbusAdu
import org.sba_research.cpsstatereplication.model.network.modbus.request.{ModbusReadHoldingRegistersRequest, ModbusWriteMultipleRegistersRequest}
import org.sba_research.cpsstatereplication.model.node.{Hmi, Inet, Plc, Program, Node => CpsNode}
import org.sba_research.cpsstatereplication.model.stimulus.AmlStimuli

import scala.language.implicitConversions
import scala.xml._

object AmlStimuliParser {

  def main(args: Array[String]): Unit = {
    val cfg = ConfigFactory.load
    val result = parseFromFile(cfg.getString("cpsReplay.aml.path"))
    println(result)
  }

  def parseFromFile(path: String): AmlStimuli = parse(XML.loadFile(path))

  def parseFromFile(file: File): AmlStimuli = parse(XML.loadFile(file))

  def parseFromString(string: String): AmlStimuli = parse(XML.loadString(string))

  // Map[SystemLog, NodeStimulusLink]

  private def parse(elem: Elem): AmlStimuli = {

    val refUriTagPattern = """^(.*)#(.*)""".r

    implicit def nodeSeqToAmlNodeSeq(nodeSeq: NodeSeq): AmlNodeSeq = new AmlNodeSeq(nodeSeq)

    /**
      * Helper method to retrieve the `startingAddress` and `quantityOfRegisters` values from AML.
      *
      * @param n the node to extract the values from
      * @return optionally, a tuple composed of the `startingAddress` and `quantityOfRegisters`
      */
    def getCommonModbusRequestValues(n: Node): Option[(Seq[Byte], Seq[Byte])] = {
      val children = NodeSeq.fromSeq(n.child)
      val startingAddress = children findAttributeValueByName StartingAddr
      val quantityOfRegisters = children findAttributeValueByName QuantReg
      for {
        startAddrStr <- startingAddress
        quantReqsStr <- quantityOfRegisters
        startAddr <- Some(BigInt(startAddrStr).toByteArray.toSeq)
        quantReqs <- Some(BigInt(quantReqsStr).toByteArray.toSeq)
      } yield (startAddr, quantReqs)
    }

    /**
      * This method is used to retrieve all internal links from an `InternalElement` node.
      *
      * @param n the `InternalElement` node
      * @return a sequence of internal links
      */
    def getInternalLinksFromInternalElementNode(n: Node): Seq[InternalLink] =
      n.child
        .filter(_.label == IL.toString)
        .flatMap { il =>
          for {
            name <- il \ s"@${Name.toString}"
            refA <- il \ s"@${RefPartnerSideA.toString}"
            refB <- il \ s"@${RefPartnerSideB.toString}"
          } yield InternalLink(name.toString, refA.toString, refB.toString)
        }

    /**
      * Groups a reference (format: `{ID}:tagValue`) in a (ID, tagValue) tuple.
      *
      * @param ref the complete reference
      * @return optionally a tuple that represents the reference
      */
    def groupRefPartnerSide(ref: String): Option[(String, String)] = {
      val r = """\{(.*)\}:(.*)""".r
      ref match {
        case r(id, value) => Some((id, value))
        case _ => None
      }
    }

    /**
      * Retrieves the network info (IP, netmask & MAC) from internal element.
      *
      * @param ie the internal element to parse the network info from
      * @return optionally, a tuple containing an instance of [[Inet]] and a String representing the MAC
      */
    def getNetworkInfoFromNodeInternalElement(ie: Node): Option[(Inet, String)] = {
      val physicalNetworkNode = NodeSeq.fromSeq(ie.child)
        .filterByElementTagAndAttribute(ExternalInterface.toString, (RefBaseClassPath.toString, CommPhysicalSocket.toString))

      /* We currently only support one NIC per device. */
      physicalNetworkNode.headOption.flatMap { p =>
        val networkInfo = p.child.flatMap { a =>
          val name = a.attribute(Name.toString).map(_.text)
          val value: Option[String] = a.child.find(_.label == Value.toString).map(_.text)
          /* Return attribute name and value, e.g., ("mac", "28:63:36:9b:0a:ee") */
          for {
            a <- name
            b <- value
          } yield (a, b)
        }
        /* Build result. */
        for {
          ip <- networkInfo.find(_._1 == Ip.toString).map(_._2)
          netmask <- networkInfo.find(_._1 == Netmask.toString).map(_._2)
          mac <- networkInfo.find(_._1 == Mac.toString).map(_._2)
        } yield (Inet(ip, netmask), mac)
      }
    }

    /**
      * Used to retrieve the tag name from a node sequence containing tag data.
      *
      * @param attrs the node sequence to parse from
      * @return optionally, the tag name
      */
    def getTagNameByAttrs(attrs: NodeSeq): Option[String] = {
      attrs
        /* Get XML element with attribute 'Name="refURI"' as element. */
        .find(_.attribute(Name.toString).exists(_.text == RefUri.toString))
        /* Retrieve tag name by parsing 'Value' XML element. */
        .flatMap { rf =>
        (NodeSeq.fromSeq(rf.child) \\ Value.toString).text match {
          case refUriTagPattern(_, tag) => Some(tag)
          case _ => None
        }
      }
    }

    /**
      * Used to retrieve the tag type from a node sequence containing tag data.
      *
      * @param attrs the node sequence to parse from
      * @return optionally, the tag type
      */
    def getTagTypeByAttrs(attrs: NodeSeq): Option[String] = {
      attrs
        /* Get XML element with attribute 'Name="type"' as element. */
        .find(_.attribute(Name.toString).exists(_.text == DataType.toString))
        /* Retrieve tag data type by parsing 'Value' XML element. */
        .map { rf => (NodeSeq.fromSeq(rf.child) \\ Value.toString).text }
    }

    /**
      * This method is used to get a map of nodes by retrieving the references of internal links.
      *
      * @param mbReqs the sequence of Modbus requests
      * @return a map of nodes
      */
    def getNodesMapByInternalLinkRef(mbReqs: Seq[(ModbusAdu, Seq[InternalLink])]): Map[String, (CpsNode, Node)] = {
      /* Flatten internal links & retrieve reference ID.
       * 1. Pack all internal links in a sequence
       * 2. For each internal link, create a new list containing only the reference ID
       * 3. Flatten the sequence
       * 4. Return sequence without duplicates
       */
      val refIds = mbReqs.flatMap(_._2).flatMap { il =>
        List(groupRefPartnerSide(il.refA).map(_._1), groupRefPartnerSide(il.refB).map(_._1))
      }.flatten.distinct

      refIds.flatMap { id =>
        /* Find an InternalElement in AML that satisfies PLC role requirement and matches ID from InternalLink reference. */
        val plcNode = ((elem \\ IE.toString) filterByChildRoleExists { attrValue: String => attrValue.equals(RoleReqPlc.toString) })
          .find(n => (NodeSeq.fromSeq(n.child) \\ s"@${Id.toString}").exists(_.text == id))
        val node: Option[(CpsNode, Node)] = plcNode match {
          case Some(p) =>
            /* Parse PLC properties from AML. */
            val nodeName = p.attribute(Name.toString).map(_.text)
            val netInfo = getNetworkInfoFromNodeInternalElement(p)
            /* Parse variables to build program instance. */
            /* Get all child elements with `RefBaseClassPath` == `AutomationMLInterfaceClassLibLogics/VariableInterface` */
            val variableIntfNodes = NodeSeq.fromSeq(p.child)
              .filterByElementTagAndAttribute(ExternalInterface.toString, (RefBaseClassPath.toString, VariableInterface.toString))
            val tags = variableIntfNodes.flatMap { v =>

              val tagAttrs = NodeSeq.fromSeq(v.child) \\ Attr.toString

              /* Retrieve tag name + data type & return. */
              for {
                tagName <- getTagNameByAttrs(tagAttrs)
                tagType <- getTagTypeByAttrs(tagAttrs)
              } yield tagName -> tagType

            }.toMap

            for {
              name <- nodeName
              net <- netInfo.map(_._1)
              mac <- netInfo.map(_._2)
            } yield (Plc(name, net, mac, Program(tags)), p)

          case None =>
            /* InternalElement is not a PLC, maybe HMI... */
            val hmiNode = ((elem \\ IE.toString) filterByChildRoleExists { attrValue: String => attrValue.equals(RoleReqHmi.toString) })
              .find(n => (NodeSeq.fromSeq(n.child) \\ s"@${Id.toString}").exists(_.text == id))
            hmiNode.flatMap { h =>
              /* Parse PLC properties from AML. */
              val nodeName = h.attribute(Name.toString).map(_.text)
              val netInfo = getNetworkInfoFromNodeInternalElement(h)
              val variableIntfNodes = NodeSeq.fromSeq(h.child)
                .filterByElementTagAndAttribute(ExternalInterface.toString, (RefBaseClassPath.toString, HmiVariableInterface.toString))
              /* Parse tags. */
              val tags = variableIntfNodes.flatMap { vari =>
                for {
                  tagName <- vari.attribute(Name.toString).map(k => k.text)
                  tagType <- getTagTypeByAttrs(NodeSeq.fromSeq(vari.child))
                } yield tagName -> tagType
              }.toMap
              for {
                name <- nodeName
                net <- netInfo.map(_._1)
                mac <- netInfo.map(_._2)
              } yield (Hmi(name, net, mac, Program(tags)), h)
            }
        }
        node.map(n => (id, n))
      }.toMap

    }

    /**
      * Used to retrieve the tag name from the `InternalLink` that is referenced in a Modbus request.
      * For HMI's this process is straight forward, since variables are directly referenced. However,
      * for PLC's we have two more indirections before getting PLC vars.
      *
      * The referencing scheme is as follows:
      *
      * ***************************     ****************     **************     *******************
      * * Modbus Request PLC Ref. * --> * Modbus Table * --> * Modbus Map * --> * PLC Program Var *
      * ***************************     ****************     **************     *******************
      *
      * @param n   the tuple containing the same node in different representations ([[CpsNode]], [[Node]])
      * @param ref the tuple containing the reference `(id, tagName)`
      * @return optionally, the tag name of the node's program
      */
    def getTagNameByInternalLink(n: (CpsNode, Node), ref: (String, String)): Option[String] = n match {
      case (plc: Plc, node: Node) =>
        val cmpRef = s"{${ref._1}}:${ref._2}"
        /* 1. Parse ModbusMap. */
        val modbusMap = (node \\ IE.toString) filterByChildRoleExists { attrValue: String => attrValue.equals(RoleReqModbusMap.toString) }
        val modbusMapLinks = (modbusMap \\ InternalLink.toString()).flatMap { il =>
          for {
            name <- il \ s"@${Name.toString}"
            refA <- il \ s"@${RefPartnerSideA.toString}"
            refB <- il \ s"@${RefPartnerSideB.toString}"
          } yield InternalLink(name.toString, refA.toString, refB.toString)
        }
        modbusMapLinks.find(il => il.refA == cmpRef || il.refB == cmpRef).flatMap { mb =>
          val plcTagRef = cmpRef match {
            case mb.refA => mb.refB
            case mb.refB => mb.refA
          }
          groupRefPartnerSide(plcTagRef) match {
            case Some((id, tagName)) =>
              /* 2. Parse variables from program. */
              val plcProgramVarNodes =
              /* We could also ... */
                ((NodeSeq.fromSeq(node.child) \\ IE.toString)
                  .filter(progNodes => progNodes.attribute(Id.toString)
                    .exists(_.text == id)
                    /* ... skip this part entirely and call:
                     * `.filterByElementTagAndAttribute(ExternalInterface.toString,
                     * (RefBaseClassPath.toString, VariableInterface.toString))` right away...
                     */
                  ) \\ ExternalInterface.toString)
                  .filter(_.attribute(Name.toString).exists(_.text == tagName))
              val tag = plcProgramVarNodes.flatMap { v =>
                (NodeSeq.fromSeq(v.child) \\ Attr.toString)
                  /* Get XML element with attribute 'Name="refURI"' as element. */
                  .find(_.attribute(Name.toString).exists(_.text == RefUri.toString))
                  .flatMap { rf =>
                    /* Retrieve tag name by parsing 'Value' XML element. */
                    (NodeSeq.fromSeq(rf.child) \\ Value.toString).text match {
                      /* 3. Look up PLC var. */
                      case refUriTagPattern(_, t) if plc.program.tags.contains(t) => Some(t)
                      case _ => None
                    }
                  }
              }.headOption
              tag
            case _ => None
          }
        }
      case (hmi: Hmi, _) if hmi.program.tags.contains(ref._2) => Some(ref._2)
    }


    /**
      * Returns a Map containing Modbus requests (or responses) that map to network stimulus links.
      *
      * @param elem the AML root element
      * @return a Map representing the Modbus network stimuli
      */
    def getModbusNetworkStimuli(elem: Elem): Map[ModbusAdu, Seq[NetworkStimulusLink]] = {

      /* 1. Get all IE elements with
       * child elements == `<RoleRequirements RefBaseRoleClassPath="ModbusRoleClassLib/ModbusRequests/...` />
       */
      val amlModbusRequests = (elem \\ IE.toString) filterByChildRoleExists { attrValue: String =>
        attrValue.startsWith(MbReqs.toString) &&
          /* We have to filter out the root element of 'ModbusRequests'. */
          !attrValue.equals(MbReqs.toString)
      }

      /* 2. Split requests by function codes (via roles). */
      val amlModbus0x03Requests = amlModbusRequests filterByChildRoleExists { attrValue: String => attrValue.equals(MbReqs0x03.toString) }
      val amlModbus0x10Requests = amlModbusRequests filterByChildRoleExists { attrValue: String => attrValue.equals(MbReqs0x10.toString) }

      /* 3. Map AML Modbus requests, including internal links to our internal data structure. */
      val modbus0x03Requests: Seq[(ModbusAdu, Seq[InternalLink])] = amlModbus0x03Requests.flatMap { r =>
        getCommonModbusRequestValues(r).map { case (startAddr, quantReqs) =>
          (
            ModbusReadHoldingRegistersRequest(startAddr, quantReqs),
            getInternalLinksFromInternalElementNode(r)
          )
        }
      }
      val modbus0x10Requests: Seq[(ModbusAdu, Seq[InternalLink])] = amlModbus0x10Requests.flatMap { r =>
        getCommonModbusRequestValues(r).map { case (startAddr, quantReqs) =>
          /* Register values are not considered in AML. */
          (
            ModbusWriteMultipleRegistersRequest(startAddr, quantReqs),
            getInternalLinksFromInternalElementNode(r)
          )
        }
      }

      val modbusRequests = modbus0x03Requests ++ modbus0x10Requests

      /* 4. Filter out duplicate IDs that reference nodes & populate these nodes. */
      val nodesMap: Map[String, (CpsNode, Node)] = getNodesMapByInternalLinkRef(modbusRequests)

      /* 5. Build map that links Modbus packets to `Seq[NetworkStimulusLink]` and return result. */
      modbusRequests.map { case (mbr, ils) =>
        /* Map each internal link... */
        val links: Seq[NetworkStimulusLink] = ils.flatMap { il =>
          for {
            /* Source */
            refA <- groupRefPartnerSide(il.refA)
            /* Destination */
            refB <- groupRefPartnerSide(il.refB)
          } yield {
            for {
              src <- nodesMap.get(refA._1)
              /* 'RefA' must be HMI for a stimulus. */
              hmi <- if (src._1.isInstanceOf[Hmi]) Some(src) else None
              srcTagName <- getTagNameByInternalLink(hmi, refA)
              dst <- nodesMap.get(refB._1)
              dstTagName <- getTagNameByInternalLink(dst, refB)
            } yield {
              NetworkStimulusLink(
                src = NodeStimulusLink(src._1, srcTagName),
                dst = NodeStimulusLink(dst._1, dstTagName)
              )
            }
          }
        }.flatten
        mbr -> links
      }
        /* Filter out Modbus requests that do not have any network stimulus links. */
        .filter(_._2.nonEmpty)
        .toMap
    }

    /**
      * Returns a set of system log stimuli.
      *
      * @param elem the AML root element
      * @return a set of system log stimuli
      */
    def getSystemLogsStimuli(elem: Elem): Set[SystemLog] = {
      /**
        * Returns the a sequence of logs (= stimuli) that are associated to RFID readers.
        *
        * @param elem the AML root element
        * @return a sequence of RFID reader logs that represent stimuli
        */
      def getRfidReaderLogsStimuli(elem: Elem): Seq[RfidReaderLog] = {
        /* 1. Get all IE elements with
         * child elements == `<RoleRequirements RefBaseRoleClassPath="IIoTRoleClassLib/RFIDReaderMQTTWiFi` />
         */
        val rfidReadersIEs = (elem \\ IE.toString) filterByChildRoleExists { attrValue: String =>
          attrValue.equals(RfidReaderMqttWiFiReqs.toString)
        }
        /* 2. Extract name of RFID reader & program node. Tuple: (name, programNode). */
        val rfidReadersProgram: Seq[(String, NodeSeq)] = rfidReadersIEs.flatMap { node =>
          val name = node.attribute(Name.toString).map(_.text)
          val programNode = (node.nodeSeq \\ IE.toString) filterByChildRoleExists { attrValue: String =>
            attrValue.equals(RfidReaderMqttProgramReqs.toString)
          }
          val progNode = Option(programNode.isEmpty).collect { case false => programNode }
          for {
            n <- name
            p <- progNode
          } yield (n, p)
        }
        /* 3. Map every RFID reader to a stimulus if it has any external interfaces (that would indicate that they are ready to read tags).  */
        val rfidReaderStimuli: Seq[RfidReaderLog] = rfidReadersProgram.flatMap { case (name, programNodeSeq) =>
          val hasExtIntf = (programNodeSeq \\ ExternalInterface.toString).nonEmpty
          if (hasExtIntf) Some(RfidReaderLog(name, "", ""))
          else None
        }
        rfidReaderStimuli
      }
      /* Return result. */
      getRfidReaderLogsStimuli(elem).toSet
    }

    /* Return result. */
    AmlStimuli(getModbusNetworkStimuli(elem), getSystemLogsStimuli(elem))
  }

}

class AmlNodeSeq(val nodeSeq: NodeSeq) {

  private def attrFilterByExists(p: MetaData => Boolean): NodeSeq = nodeSeq.filter(_.attributes.exists(p))

  def attrEquals(attr: (String, String)): NodeSeq =
    attrFilterByExists { p => p.key == attr._1 && p.value.text == attr._2 }

  def attrStartsWith(attr: (String, String)): NodeSeq =
    attrFilterByExists { p => p.key == attr._1 && p.value.text.startsWith(attr._2) }

  /**
    * Filters a node sequence by checking if any child exists that has `RoleRequirements`
    * as an element with the attribute `RefBaseRoleClassPath` as a key and value that matches
    * the predicate which is passed as a parameter.
    *
    * @param f the predicate to check for the `RefBaseRoleClassPath` attribute's value.
    * @return a filtered sequence of nodes
    */
  def filterByChildRoleExists(f: (String) => Boolean): NodeSeq =
    nodeSeq
      .filter(
        _.child.exists { c =>
          c.label == RoleReq.toString &&
            c.attributes.exists { a =>
              a.key == RefBaseRole.toString && f(a.value.text)
            }
        }
      )

  /**
    * Filters a node sequence by a provided tag and an attribute pair (key, value).
    *
    * @param elemTag   the element tag
    * @param attribute the attribute tuple containing the key, value
    * @return a filtered node sequence
    */
  def filterByElementTagAndAttribute(elemTag: String, attribute: (String, String)): NodeSeq =
    (nodeSeq \\ elemTag).filter { n =>
      n.attributes.exists { a =>
        a.key == attribute._1 && a.value.text == attribute._2
      }
    }

  def findAttributeValueByName(amlVal: AmlVal): Option[String] = findAttributeValueByName(amlVal.toString)

  /**
    * Finds the text of the XML child element &lt;Value&gt;foobar&lt;/Value&gt; which is the parent of an XML element that has the
    * tag label `Attribute` and the attribute key `Name` with a matching value, provided as a parameter.
    *
    * @param name the value of the `Name` attribute to match
    * @return optionally the content of an AML attribute's value
    */
  def findAttributeValueByName(name: String): Option[String] =
    nodeSeq.find { a =>
      a.label == Attr.toString && a.attributes.exists { attr =>
        attr.key == Name.toString && attr.value.text == name
      }
    }.flatMap(_.child.find(_.label == Value.toString).map(_.text))

}

// scalastyle:off
/**
  * Object to store AML values.
  */
object AmlVals {

  sealed abstract class AmlVal(val name: String) {
    override def toString: String = name
  }

  case object IE extends AmlVal("InternalElement")

  case object RoleReq extends AmlVal("RoleRequirements")

  case object RefBaseRole extends AmlVal("RefBaseRoleClassPath")

  case object MbReqs extends AmlVal("ModbusRoleClassLib/ModbusRequests")

  case object MbReqs0x03 extends AmlVal("ModbusRoleClassLib/ModbusRequests/ModbusReadHoldingRegisters")

  case object MbReqs0x10 extends AmlVal("ModbusRoleClassLib/ModbusRequests/ModbusWriteMultipleRegisters")

  case object StartingAddr extends AmlVal("startingAddress")

  case object Attr extends AmlVal("Attribute")

  case object Name extends AmlVal("Name")

  case object Value extends AmlVal("Value")

  case object QuantReg extends AmlVal("quantityOfRegisters")

  case object IL extends AmlVal("InternalLink")

  case object RefPartnerSideA extends AmlVal("RefPartnerSideA")

  case object RefPartnerSideB extends AmlVal("RefPartnerSideB")

  case object RoleReqPlc extends AmlVal("AutomationMLCSRoleClassLib/ControlEquipment/Controller/PLC")

  case object RoleReqHmi extends AmlVal("AutomationMLExtendedRoleClassLib/HMI")

  case object ExternalInterface extends AmlVal("ExternalInterface")

  case object CommPhysicalSocket extends AmlVal("CommunicationInterfaceClassLib/CommunicationPhysicalSocket")

  case object RefBaseClassPath extends AmlVal("RefBaseClassPath")

  case object Mac extends AmlVal("mac")

  case object Ip extends AmlVal("ip")

  case object Netmask extends AmlVal("netmask")

  case object Id extends AmlVal("ID")

  case object VariableInterface extends AmlVal("AutomationMLInterfaceClassLibLogics/VariableInterface")

  case object HmiVariableInterface extends AmlVal("LogicalDeviceInterfaceClassLib/HMIVariableInterface")

  case object RoleReqModbusMap extends AmlVal("ModbusRoleClassLib/ModbusMap")

  case object RefUri extends AmlVal("refURI")

  case object DataType extends AmlVal("type")

  case object DataTypeInt extends AmlVal("int")

  case object DataTypeBool extends AmlVal("boolean")

  case object RfidReaderMqttWiFiReqs extends AmlVal("IIoTRoleClassLib/RFIDReaderMQTTWiFi")

  case object RfidReaderMqttProgramReqs extends AmlVal("ProgramRoleClassLib/RFIDReaderMQTTProgram")

}

// scalastyle:on