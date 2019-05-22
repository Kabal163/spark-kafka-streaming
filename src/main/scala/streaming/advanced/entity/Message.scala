package streaming.advanced.entity

case class Message(   creationTime: Integer,
                            userId: Integer,
                           content: String,
                          hashtags: Array[String]) {}

