package streaming.advanced.serde

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import streaming.advanced.entity.Message

object Bytes2MessageDeserializer {

    var mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)

    def deserialize(data: Array[Byte]): Message = {
        mapper.readValue[Message](data)
    }

}
