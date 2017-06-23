package radio

case class Song(timestamp:Int, humanDate:Long, year:Int, month:Int, day:Int, hour:Int, minute: Int, artist:String, allArtists: String, title:String, radio:String)

case class SimpleSong(title: String, artist: String, radio: String)

case class SimpleSongAggregation(title: String, artist: String, radio: String, count: Long)