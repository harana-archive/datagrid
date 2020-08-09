package com.harana.designer.frontend.utils

import io.circe.Decoder
import sttp.client._
import sttp.client.circe._
import com.harana.sdk.utils.CirceCodecs._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object Http {

  implicit val sttpBackend = FetchBackend()

  def get[T](suffix: String)(implicit decoder: Decoder[T]): Future[Option[T]] = {
    val url = s"http://localhost:8080/api/$suffix"
    println(s"Making request to: $url")

    val request = basicRequest.get(uri"$url").response(asJson[T]).send().map(_.body)
    request.onComplete { response =>
      response.get match {
        case Left(e) => e.printStackTrace()
        case Right(s) => {}
      }
    }
    request.map(_.toOption)
  }
}