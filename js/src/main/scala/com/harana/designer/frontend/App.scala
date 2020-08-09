package com.harana.designer.frontend

import java.util.concurrent.atomic.AtomicReference

import com.harana.designer.frontend.apps.ui.JupyterPage
import com.harana.ui.external.axui_data_grid.DataGridCSS
import com.harana.ui.external.blueprint.core.BlueprintCoreCSS
import com.harana.ui.external.blueprint.datetime.BlueprintReactDayPickerCSS
import com.harana.ui.external.blueprint.icons.BlueprintIconsCSS
import com.harana.ui.external.emoji.EmojiCSS
import com.harana.ui.external.helmet.Helmet
import com.harana.ui.external.history.History
import com.harana.ui.external.react_router.{Route, Router, Switch}
import com.harana.ui.external.uppy.{ReactCoreCSS, ReactDashboardCSS}
import com.harana.designer.frontend.flow.{FlowHandler, FlowStore}
import com.harana.designer.frontend.flowlist.FlowListStore.{FlowListHandler, FlowListState}
import com.harana.designer.frontend.flow.FlowStore.{FlowState, LoadFlowExecution}
import com.harana.designer.frontend.flow.ui.FlowsPage
import com.harana.designer.frontend.flowlist.FlowListStore
import com.harana.designer.frontend.models.EventBusMessage
import com.harana.designer.frontend.utils.Diode
import com.harana.designer.frontend.pages._
import com.harana.sdk.models.designer.flow.FlowExecution
import diode.{Circuit, ModelRW}
import io.circe.parser.decode
import slinky.core.ReactComponentClass._
import slinky.core.annotations.react
import slinky.core.facade.React
import slinky.core.{CustomAttribute, FunctionalComponent}
import slinky.web.html._
import typings.std
import typings.vertx3EventbusClient.mod.{^ => EventBus}

import scala.scalajs.js
import scala.scalajs.js.Dynamic.global

@react object App {

  type Props = Unit
  val diodeContext = React.createContext[Circuit[State]](Circuit)
  val didRegisterHandlers = new AtomicReference[Boolean](false)

  val component = FunctionalComponent[Props] { _ =>

    Circuit.dispatch(FlowStore.Init)

//    new Timer().scheduleAtFixedRate(new java.util.TimerTask {
//      def run(): Unit = {
//          Circuit.dispatch(StopFlow)
//        }
//      }
//    }, 0L, 5000L)

    diodeContext.Provider(Circuit)(
      Router(history = History.createBrowserHistory())(
        div(
          Helmet(
            meta(new CustomAttribute[String]("charSet") := "utf-8"),
            meta(name := "viewport", content := "width=device-width, initial-scale=1, shrink-to-fit=no"),
            meta(name := "theme-color", content := "#000000"),
            link(rel := "manifest", href := "/manifest.json"),
            link(rel := "shortcut icon", href := "/favicon.ico"),
            style(`type` := "text/css")(BlueprintCoreCSS.toString),
            style(`type` := "text/css")(BlueprintReactDayPickerCSS.toString),
            style(`type` := "text/css")(BlueprintIconsCSS.toString),
            style(`type` := "text/css")(DataGridCSS.toString),
            style(`type` := "text/css")(EmojiCSS.toString),
            style(`type` := "text/css")(ReactCoreCSS.toString),
            style(`type` := "text/css")(ReactDashboardCSS.toString)
          ),
          Switch(
            Route("/", JupyterPage.component, exact = true),
            Route("/flows", FlowsPage.component),
            Route("/data", DataSourcesPage),
            Route("/projects", FileBrowserPage),
            Route("/apps/jupyter", JupyterPage.component),
            Route("/settings", SettingsPage),
            Route("/snippets", SnippetsPage),
            Route("/settings", SettingsPage),
            Route("/harana-cloud", FileBrowserPage),
            Route("*", JupyterPage.component)
          )
        )
      )
    )
  }

  val eventBus: EventBus = new EventBus("http://localhost:8080/eventbus") {
    override def onopen() = {
      enablePing(true)
      enableReconnect(true)

      if (!didRegisterHandlers.get) {
        App.Circuit.dispatch(FlowStore.InitEventBus)
        App.eventBus.registerHandler("server.test2", null, (error: std.Error, result: js.Any) => {
          val body = result.asInstanceOf[EventBusMessage].body
          decode[FlowExecution](body) match {
            case Left(x) => global.console.dir(x.asInstanceOf[js.Any])
            case Right(x) => Circuit.dispatch(LoadFlowExecution(x))
          }
        })

        didRegisterHandlers.set(true)
      }
   }
    override def onerror(error: std.Error) = global.console.dir(error)
  }

  def state[T](selector: ModelRW[App.State, T]) =
    Diode.use(App.diodeContext, selector)

  case class State(flow: FlowState, flowList: FlowListState)

  object Circuit extends Circuit[State] {
    def initialModel = State(
      FlowStore.initialState,
      FlowListStore.initialState
    )

    override val actionHandler: HandlerFunction = foldHandlers(
      new FlowHandler,
      new FlowListHandler
    )
  }
}