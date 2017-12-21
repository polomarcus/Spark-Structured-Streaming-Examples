package radio

import java.sql.Timestamp

case class Song(timestamp: Long, humanDate:Long, year:Int, month:Int, day:Int, hour:Int, minute: Int, artist:String, allArtists: String, title:String, radio:String)

case class SimpleSong(title: String, artist: String, radio: String)

case class SimpleSongAggregation(title: String, artist: String, radio: String, count: Long)

case class SimpleSongAggregationKafka(topic: String, partition: Int, offset: Long, timestamp: Timestamp, radioCount: SimpleSongAggregation)

case class ArtistAggregationState(artist: String, count: Long)