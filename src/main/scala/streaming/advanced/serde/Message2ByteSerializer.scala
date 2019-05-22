package streaming.advanced.serde

import java.util

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.kafka.common.serialization.Serializer
import org.slf4j.LoggerFactory
import streaming.advanced.entity.Message

class Message2ByteSerializer extends Serializer[Message] {

    val log = LoggerFactory.getLogger(classOf[Message2ByteSerializer])

    var mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

    override def serialize(s: String, message: Message): Array[Byte] = try
        mapper.writeValueAsBytes(message)
    catch {
        case e: JsonProcessingException =>
            log.error("Error while message serialization: " + e)
            throw new IllegalArgumentException("Error while message serialization", e)
    }

    override def close(): Unit = {}
}
