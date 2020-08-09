package com.harana.designer.frontend.flow

import com.harana.designer.frontend.App
import com.harana.designer.frontend.flow.FlowStore._
import com.harana.designer.frontend.flow.ui.{FlowLink, FlowNode}
import com.harana.sdk.models.designer.flow._
import com.harana.sdk.utils.CirceCodecs._
import com.harana.ui.external.diagrams._
import com.harana.designer.frontend.utils.Http
import diode._
import typings.std
import io.circe.parser._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.scalajs.js

class FlowHandler extends ActionHandler(App.Circuit.zoomTo(_.flow)) {
  override def handle: PartialFunction[Any, ActionResult[App.State]] = {

    case Nothing =>
      updated(value)

    case Init =>
      println("Initing")
      effectOnly(
        Effect(Http.get[List[ActionTypeInfo]]("flows/actionTypes").map(at => LoadActionTypes(at.getOrElse(List())))) >>
        Effect(Http.get[Flow](s"flows/cipmp22j15ZoJgfcD2zGTAaoj0Y=").map(f => if (f.isDefined) LoadFlow(f.get) else Nothing)) +
        Effect(Http.get[FlowExecution](s"execution/flows/cipmp22j15ZoJgfcD2zGTAaoj0Y=").map(fe => if (fe.isDefined) LoadFlowExecution(fe.get) else Nothing))
      )


    case InitEventBus =>
      updated(value)


    case LoadActionTypes(actionTypes) =>
      updated(value.copy(actionTypes = actionTypes))


    case LoadFlow(flow) =>
      println(s"Loading Flow with title: ${flow.title}")
      val model = new DiagramModel(defaultCanvasModelOptions)
      val engine = ReactDiagrams()

      try {
        val actions = flow.actions.map(a => a.id -> new FlowNode(a, value.actionTypes.find(_.id == a.actionTypeId).get)).toMap
        actions.values.foreach(model.addNode)

        val links = flow.links.map { link =>
          val fromAction = flow.actions.find(_.id == link.fromAction).get
          val fromPort = actions(link.fromAction).outputPorts(link.fromPort.name)
          val toAction = flow.actions.find(_.id == link.toAction).get
          val toPort = actions(link.toAction).inputPorts(link.toPort.name)
          new FlowLink(fromAction, fromPort, toAction, toPort)
        }
        links.foreach(model.addLink)

        engine.setModel(model)
        updated(value.copy(flow = Some(flow), engine = Some(engine), actions = actions.values.toSet, links = links.toSet))
      } catch {
        case e: Exception => e.printStackTrace()
          updated(value)
      }


    case LoadFlowExecution(flowExecution) =>
      println(s"Loading FlowExecution for flow: ${flowExecution.flowId}")
      if (flowExecution.activeActionId.isDefined) {
        value.links.filter(_.fromAction.id == flowExecution.activeActionId.get).foreach(_.setSelected(true))
      }
      updated(value.copy(flowExecution = Some(flowExecution), links = value.links))


    case RunFlow =>
      updated(value.copy(isRunning = true, selectedTab = "run"))


    case StopFlow =>
      value.links.foreach(_.setSelected(false))
      updated(value.copy(isRunning = false))


    case UpdateFlow =>
      null


    case SelectTab(tab) =>
      null


    case ZoomIn =>
      val model = value.engine.get.getModel()
      model.setZoomLevel(model.getZoomLevel() + 5)
      updated(value.copy(engine = value.engine))


    case ZoomOut =>
      val model = value.engine.get.getModel()
      model.setZoomLevel(model.getZoomLevel() - 5)
      updated(value.copy(engine = value.engine))


    case AddAction(event) =>
      value.selectedActionType match {
        case Some(x) =>
          val node = new DefaultNodeModel(new DefaultNodeModelOptions {
            override val name = x.title
            //override val color = x.color.html
          })

          val position = value.engine.get.getRelativeMousePoint(new CanvasPosition {
            override val clientX = event.clientX.toInt
            override val clientY = event.clientY.toInt
          })

          node.setPosition(position.x, position.y)
          node.addOutPort("Out")
          value.engine.get.getModel().addNode(node)
          updated(value.copy(engine = value.engine))
      }


    case SelectAction(action) =>
      updated(value.copy(selectedAction = Some(action), selectedTab = "parameters"))


    case SelectActionType(actionType) =>
      updated(value.copy(selectedActionType = Some(actionType)))


    case DeselectAllActions =>
      updated(value.copy(selectedAction = None, selectedTab = "actionTypes"))


    case MoveAction(action) =>
      updated(value)
  }
}