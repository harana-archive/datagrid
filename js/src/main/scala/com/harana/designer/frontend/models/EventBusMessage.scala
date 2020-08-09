package com.harana.designer.frontend.models

import scala.scalajs.js

@js.native
trait EventBusMessage extends js.Object {
  val `type`: String = js.native
  val address: String = js.native
  val body: String = js.native
}