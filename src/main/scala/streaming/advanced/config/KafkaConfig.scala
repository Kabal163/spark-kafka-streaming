package streaming.advanced.config

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.LongDeserializer
import streaming.advanced.serde.{Bytes2MessageDeserializer, Message2ByteSerializer}

object KafkaConfig {

//    def getConsumerConfig(): Map[String, String] = {
//        Map(
//            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "sandbox-hdp.hortonworks.com:6667",
//            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[LongDeserializer].getName,
//            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[Bytes2MessageDeserializer].getName,
//            ConsumerConfig.GROUP_ID_CONFIG -> "group_1",
//            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
//            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
//            ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> "10")
//    }

    def getProducerConfig(): Map[String, String] = {
        Map(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "sandbox-hdp.hortonworks.com:6667",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[LongDeserializer].getName,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[Message2ByteSerializer].getName,
            ProducerConfig.CLIENT_ID_CONFIG -> s"client_${Thread.currentThread().getId}",
            ProducerConfig.ACKS_CONFIG -> "all",
            ProducerConfig.BATCH_SIZE_CONFIG -> "524288",
            ProducerConfig.BUFFER_MEMORY_CONFIG -> "33554432")
    }
}
