package streaming.advanced.serde

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.slf4j.LoggerFactory
import streaming.advanced.entity.StreamingResult

object StreamingResult2ByteSerializer  {

    val log = LoggerFactory.getLogger("StreamingResult2ByteSerializer")

    var mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)

    def serialize(result: StreamingResult): Array[Byte] = try
        mapper.writeValueAsBytes(result)
    catch {
        case e: JsonProcessingException =>
            log.error("Error while streaming result serialization: " + e)
            throw new IllegalArgumentException("Error while streaming result serialization", e)
    }
}
