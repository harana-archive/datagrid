package com.harana.designer.frontend.flow.ui

import com.harana.sdk.models.designer.flow.Action
import com.harana.ui.external.diagrams._

class FlowLink(val fromAction: Action, fromPort: PortModel[_, _], val toAction: Action, toPort: PortModel[_, _]) extends DefaultLinkModel[DefaultLinkModelGenerics, DefaultLinkModelListener](new DefaultLinkModelOptions {}) {
  this.setSourcePort(fromPort)
  this.setTargetPort(toPort)
}

//class FlowLinkModelListener(action: Action) extends LinkModelListener {
//  override def sourcePortChanged: BaseEntityEvent[LinkModel[_,_]] = println("Source changed")
//  override def targetedPortChanged: BaseEntityEvent[LinkModel[_,_]] = prin
//}