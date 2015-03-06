package util

import dispatch._, Defaults._

/**
 * Created by rhernando on 6/03/15.
 */
object UrlUtil {

  def getRealUrl(red_url: String): String ={
    val svc = url(red_url)
    val r = Http(svc > (x => x))
    val res = r()
    Option(res.getHeader("Location")) getOrElse(red_url)

  }


}
