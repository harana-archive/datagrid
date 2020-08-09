package com.harana.designer.frontend.flow.ui

import com.harana.designer.frontend.App
import com.harana.designer.frontend.App.Circuit
import com.harana.designer.frontend.flow.FlowStore._
import com.harana.designer.frontend.pages.Common
import com.harana.ui.components._
import com.harana.ui.components.elements.{HeadingItem, Icon, Page}
import com.harana.ui.components.sidebar._
import com.harana.ui.external.diagrams._
import slinky.core.FunctionalComponent
import slinky.core.annotations.react
import slinky.web.html._

@react object FlowsPage {
  type Props = Unit

  def pageContent(state: FlowState) = {
    div(className := "flow-container")(
      div(className := "flow-sidebar")(
        Sidebar(List(), sidebarTabs(state), showBorder = false, fixed = true, padding = false, separateCategories = false)
      ),
      if (state.engine.isDefined)
        div(className := "flow-grid",
            onClick := ( e => {
              //dispatcher.dispatch(DeselectAllActions)
            }),
            onDrop := (e => App.Circuit.dispatch(AddAction(e))),
            onDragOver := (e => e.preventDefault())
        )(CanvasWidget(engine = state.engine.get))
      else
        div()
    )
  }

  def headingItems(state: FlowState) =
    List(
      HeadingItem.Icon(Icon.GlyphZoomIn, LinkType.OnClick(() => App.Circuit.dispatch(ZoomIn))),
      HeadingItem.Icon(Icon.GlyphZoomOut, LinkType.OnClick(() => App.Circuit.dispatch(ZoomOut))),
      HeadingItem.Icon(if (state.isRunning) Icon.Stop2 else Icon.Play4, LinkType.OnClick(() => App.Circuit.dispatch(if (state.isRunning) StopFlow else RunFlow)))
    )

  def sidebarTabs(state: FlowState) = {
    val actionTypesCategory = SidebarSection(None, ContentSection(actionTypes(state), padding = false))
    val dataSourcesCategory = SidebarSection(None, ContentSection(actionTypes(state), padding = false))

    val actionType = state.selectedAction.flatMap(sa => state.actionTypes.find(_.id == sa.actionTypeId))
    val parametersCategory = SidebarSection(None, parameters(actionType))
    val runCategory = SidebarSection(
      None,
      ContentSection(div(
        runStatus(state.flowExecution),
        runTime(state.flowExecution),
        runHealth(state.flowExecution),
        runResources(state.flowExecution),
        runShuffle(state.flowExecution))
      ))

    List(
      Tab("actionTypes", List(actionTypesCategory), Some(Icon.Cube4), active = state.selectedTab.equals("actionTypes")),
      Tab("dataSources", List(dataSourcesCategory), Some(Icon.Archive), active = state.selectedTab.equals("dataSources")),
      Tab("parameters", List(parametersCategory), Some(Icon.Equalizer2), active = state.selectedTab.equals("parameters")),
      Tab("run", List(runCategory), Some(Icon.Chart), active = state.selectedTab.equals("run"))
    )
  }

  val component = FunctionalComponent[Unit] { _ =>
    val state = App.state(Circuit.zoomTo(_.flow))._1
    val title = state.flow.map(_.title).getOrElse("")
    Page(title, title, Some(Common.navigationDropdown), headingItems(state), Some(Common.navigationBar(Common.flowsItem)), content = pageContent(state), fixedSizeContent = true, padding = false)
  }
}