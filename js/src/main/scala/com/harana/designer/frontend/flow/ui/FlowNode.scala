package com.harana.designer.frontend.flow.ui

import com.harana.designer.frontend.App
import com.harana.designer.frontend.flow.FlowStore
import com.harana.sdk.models.designer.flow.{Action, ActionTypeInfo}
import com.harana.ui.external.diagrams.{NodeModelListener, _}

class FlowNode(action: Action, actionType: ActionTypeInfo) extends DefaultNodeModel[DefaultNodeModelGenerics, FlowNodeModelListener](
  new DefaultNodeModelOptions {
    override val name = action.title
    override val color = action.colour
  }) {

  setPosition(action.xPosition, action.yPosition)
  registerListener(new FlowNodeModelListener(action))

  val inputPorts =
    actionType.inputPorts.map(p => p.name -> addInPort[DefaultPortModelGenerics, PortModelListener](p.name)).toMap

  val outputPorts =
    actionType.outputPorts.map(p => p.name -> addOutPort[DefaultPortModelGenerics, PortModelListener](p.name)).toMap
}

class FlowNodeModelListener(action: Action) extends NodeModelListener {
  override def selectionChanged(event: SelectionChangedEvent): Unit = App.Circuit.dispatch(FlowStore.SelectAction(action))
  override def positionChanged(event: BaseEntityEvent[BasePositionModel[_,_]]): Unit = println("Position Changed: " + event.entity.getID())
  override def entityRemoved(event: BaseEntityEvent[BaseModel[_,_]]): Unit = {}
  override def lockChanged(event: BaseEntityEvent[BaseModel[_,_]]): Unit = {}
}