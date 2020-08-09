package com.harana.designer.frontend.models

sealed trait FlowTab
object FlowTab {
  case object Actions extends FlowTab
  case object Parameters extends FlowTab
  case object Execution extends FlowTab
  case object Logs extends FlowTab
}