package com.harana.designer.frontend

import org.scalajs.dom
import org.scalajs.dom.HashChangeEvent
import slinky.hot
import slinky.web.ReactDOM

import scala.scalajs.LinkingInfo
import scala.scalajs.js.annotation.{JSExport, JSExportTopLevel}

@JSExportTopLevel("Main")
object Main {

  @JSExport
  def main(args: Array[String]): Unit = {

    if (LinkingInfo.developmentMode) hot.initialize()

    val container = Option(dom.document.getElementById("root")).getOrElse {
      val elem = dom.document.createElement("div")
      elem.id = "root"
      dom.document.body.appendChild(elem)
      elem
    }

    dom.window.onhashchange = (e: HashChangeEvent) => {
      val hash = dom.window.location.hash
      if (hash.length() >= 1) {
//        val f = TodoFilter.from(hash.substring(1))
//        dispatch(SelectFilter(f))
      }
    }

    ReactDOM.render(App(), container)
  }
}