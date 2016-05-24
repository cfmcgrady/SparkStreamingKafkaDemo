package streaming.utils

/**
  * Created by chenfu_iwm on 16-5-19.
  */
case class NginxLogRecord (
                            clientIpAddress:String,         // should be an ip address, but may also be the hostname if hostname-lookups are enabled
                            remoteUser:String,              // typically '-'
                            dateTime:String,                // [day/month/year:hour:minute:second zone]
                            verb:String,                    // HTTP verb GET, POST, etc
                            URL:String,                     // Resource accessed (URL)
                            HTTPVersion:String,             // HTTP version: 1.1, 1.0
                            RequestProcessingTime:String,   // Request Time in ms
                            ReceivedBytes:String,           // Bytes received in the response
                            URLReferer:String,              // Referer URL
                            UserAgent:String,               // Which User Agent
                            UpstreamResponseTime:String,    // Upstream response time, typically '-'
                            Pipe:String,                    // Typically .
                            ResponseCode:String             // HTTP Status
                          )
class NginxLineParser extends Serializable {
  private val regex = "([^-]*)\\s+-\\s+(\\S+)\\s+\\[(\\d{2}\\/[a-zA-Z]{3}\\/\\d{4}:\\d{2}:\\d{2}:\\d{2}\\s+-\\d{4})\\]\\s+\"(.+)\"\\s+(\\d{1,}\\.\\d{3})\\s+(\\d+)\\s+\"([^\"]+)\"\\s+Agent\\[\"([^\"]+)\"\\]\\s+(-|\\d.\\d{3,})\\s+(\\S+)\\s+(\\d{1,}).*".r

  /**
    * @param record Assumed to be an Nginx access log.
    * @return An NginxLogRecord instance wrapped in an Option.
    */
  def parse(record: String): Option[NginxLogRecord] = {

    def parseRequestField(request: String): Option[(String, String, String)] = {
      request.split(" ").toList match {
        case List(a, b, c) => Some((a, b, c))
        case other => None
      }
    }

    record match {
      case regex(ip, ruser, datetime, req, reqtime, recbytes, ref, ua, upstreamtime, pipe, status) =>
        val requestTuple = parseRequestField(req)
        Some(
          NginxLogRecord(
            ip,
            ruser,
            datetime,
            if (requestTuple.isDefined) requestTuple.get._1 else "",
            if (requestTuple.isDefined) requestTuple.get._2 else "",
            if (requestTuple.isDefined) requestTuple.get._3 else "",
            reqtime,
            recbytes,
            ref,
            ua,
            upstreamtime,
            pipe,
            status
          )
        )
      case _ => None
    }
  }
}

object NginxUtils {


  val parser = new NginxLineParser
  def main(args: Array[String]): Unit = {
    val log = """192.168.0.102 - - [04/May/2015:23:02:01 -0300]  "GET /adserver/www/delivery/lg.php?bannerid=9382&campaignid=679&zoneid=1519&cb=40f1441eb8 HTTP/1.1" 0.000  43 "-" Agent["MOT-EX128 Obigo/WAP2.0 MIDP-2.0/CLDC-1.1"] - . 200"""

    val s = """192.168.0.102 - - [04/May/2015:23:02:01 -0300]  "GET """
    val e = """ HTTP/1.1" 0.000  43 "-" Agent["MOT-EX128 Obigo/WAP2.0 MIDP-2.0/CLDC-1.1"] - . 200"""
    val l = s"$s/abc$e"
    val a = parser.parse(l)
    println(a.get.URL)
  }

  def parse(record: String): Option[NginxLogRecord] = parser.parse(record)

  def produceLog(page: String): String = {
    val s = """192.168.0.102 - - [04/May/2015:23:02:01 -0300]  "GET """
    val e = """ HTTP/1.1" 0.000  43 "-" Agent["MOT-EX128 Obigo/WAP2.0 MIDP-2.0/CLDC-1.1"] - . 200"""
    s"$s$page$e"
  }
}
