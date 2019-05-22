package streaming.advanced.entity

case class StreamingResult(startTime: BigInt,
                           endTime: BigInt,
                           hashtag: String,
                           count: Int) {}
