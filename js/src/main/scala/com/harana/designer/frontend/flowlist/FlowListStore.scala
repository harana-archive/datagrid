package com.harana.designer.frontend.flowlist

import com.harana.sdk.models.designer.flow.Flow
import com.harana.designer.frontend.App
import diode.{Action => DiodeAction, _}

object FlowListStore {

  case class FlowListState()

  val initialState = FlowListState()

  case object Init extends DiodeAction
  case class CreateFlow(flow: Flow) extends DiodeAction
  case class DeleteFlow(flow: Flow) extends DiodeAction
  case class SelectFlow(flow: Flow) extends DiodeAction

  class FlowListHandler extends ActionHandler(App.Circuit.zoomTo(_.flowList)) {
    override def handle: PartialFunction[Any, ActionResult[App.State]] = {

      case Init => null

      case CreateFlow(flow) => null

      case DeleteFlow(flow) => null

      case SelectFlow(flow) => null
    }
  }
}