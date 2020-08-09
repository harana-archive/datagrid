package com.harana.designer.frontend.flow

import com.harana.designer.frontend.flow.ui.{FlowLink, FlowNode}
import com.harana.designer.frontend.models.FlowTab
import com.harana.sdk.models.designer.flow._
import com.harana.ui.external.diagrams._
import diode.{Action => DiodeAction}
import slinky.web.SyntheticMouseEvent

object FlowStore {

  case class FlowState(engine: Option[DiagramEngine],
                       actionTypes: List[ActionTypeInfo],
                       flow: Option[Flow],
                       flowExecution: Option[FlowExecution],
                       isRunning: Boolean,
                       actions: Set[FlowNode],
                       links: Set[FlowLink],
                       selectedAction: Option[Action],
                       selectedActionType: Option[ActionTypeInfo],
                       selectedTab: String)

  val initialState = FlowState(None, List(), None, None, isRunning = false, Set(), Set(), None, None, "run")

  case object Nothing extends DiodeAction
  case object Init extends DiodeAction
  case object InitEventBus extends DiodeAction
  case class LoadActionTypes(actionTypes: List[ActionTypeInfo]) extends DiodeAction
  case class LoadFlow(flow: Flow) extends DiodeAction
  case class LoadFlowExecution(flowExecution: FlowExecution) extends DiodeAction
  case object RunFlow extends DiodeAction
  case object StopFlow extends DiodeAction
  case object UpdateFlow extends DiodeAction
  case class SelectTab(tab: FlowTab) extends DiodeAction
  case object ZoomIn extends DiodeAction
  case object ZoomOut extends DiodeAction
  case class AddAction(event: SyntheticMouseEvent[_]) extends DiodeAction
  case class RunAction(action: Action) extends DiodeAction
  case class SelectAction(action: Action) extends DiodeAction
  case class SelectActionType(actionType: ActionTypeInfo) extends DiodeAction
  case class MoveAction(action: Action) extends DiodeAction
  case object DeselectAllActions extends DiodeAction
}