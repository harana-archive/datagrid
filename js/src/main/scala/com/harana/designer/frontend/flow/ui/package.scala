package com.harana.designer.frontend.flow

import com.harana.designer.frontend.App
import com.harana.designer.frontend.flow.FlowStore.{FlowState, SelectActionType}
import com.harana.designer.frontend.utils.Time
import com.harana.sdk.models.common.ParameterGroup
import com.harana.sdk.models.designer.flow._
import com.harana.sdk.models.designer.execution.AggregateMetric._
import com.harana.sdk.models.designer.execution.{AggregateMetric, ExecutionStatus}
import com.harana.ui.components.cssSet
import com.harana.ui.components.elements.{Color, Label, LabelStyle, ProgressBar, ProgressBarStyle}
import com.harana.ui.components.sidebar.ParametersSection
import slinky.core.facade.{Fragment, ReactElement}
import slinky.web.html._

import scala.util.Try

package object ui {

  def actionTypes(state: FlowState): ReactElement =
    div(className := "flow-sidebar-components")(
      state.actionTypes.map { actionType =>
        li(key := actionType.title, className := "flow-component-item", draggable := "true", onDragStart := (_ => App.Circuit.dispatch(SelectActionType(actionType))))(
          div()(
            div(className := cssSet("flow-component-square" -> true, s"bg-primary" -> true)),
            actionType.title
          )
        )
      }
    )


  def parameters(actionType: Option[ActionTypeInfo]): ReactElement =
    div(className := "flow-sidebar-components")(
      actionType match {
        case Some(at) =>
          Fragment(
            div(className := "category-content")(
              h6(at.title),
              p(at.description)
            ),
            ParametersSection(
              at.parameterTypes.map { group =>
                println(group.name)
                ParameterGroup(group.name, group.parameters)
              }
            )
          )
        case None => h1("Please select an action")
      }
    )

  def runStatus(run: Option[FlowExecution]): ReactElement =
    Fragment(
      div(h6("Status")),
      div(className := "pb-20")(
        if (run.isEmpty) {
          Label("Not yet executed", color = Some(Color.Grey400))
        }else{
          run.get.executionStatus match {
            case ExecutionStatus.None => Label("Not yet executed", color = Some(Color.Grey400))
            case ExecutionStatus.Executing =>
              ProgressBar(percentage = if (run.isDefined) 50 else 0, style = Some(ProgressBarStyle.Animated))
              span(className := "media-annotation")(if (run.isDefined) "1 minute, 36 seconds" else "")

            case ExecutionStatus.Failed => Label("Failed", color = Some(Color.Red400))
            case ExecutionStatus.Killed => Label("Killed", color = Some(Color.Red400))
            case ExecutionStatus.TimedOut => Label("Timed Out", color = Some(Color.Red400))
            case ExecutionStatus.Cancelled => Label("Cancelled", color = Some(Color.Grey400))
            case ExecutionStatus.PendingCancellation => Label("Waiting to Cancel", color = Some(Color.Grey400))
            case ExecutionStatus.PendingExecution => Label("Waiting to Execute", color = Some(Color.Grey400))
            case ExecutionStatus.Paused => Label("Paused", color = Some(Color.Orange400))
            case ExecutionStatus.Succeeded => Label("Success", color = Some(Color.Green400))
          }
        }
      )
    )

  def runTime(run: Option[FlowExecution]): ReactElement = {
    val info = run.flatMap(_.info)
    Fragment(
      div(h6("Timing")),
      ul(className := "media-list media-list-linked pb-15")(
        row("Start", Try(Time.format(info.get.startTime)).toOption),
        row("Finish", Try(Time.format(info.get.endTime)).toOption),
        row("Duration", Try(s"${Time.pretty(info.get.startTime, info.get.endTime)}").toOption)
      )
    )
  }


  def runHealth(run: Option[FlowExecution]): ReactElement =
    Fragment(
      div(h6("Health")),
      ul(className := "media-list media-list-linked pb-15")(
        healthRow("Disk Spill", "Percentage of time spent in GC", run.map(_ => "success")),
        healthRow("Driver Wastage", "Amount of time wasted in the Driver", run.map(_ => "danger")),
        healthRow("Executor Wastage", "Percentage of time spent in GC", run.map(_ => "warning")),
        healthRow("Failed Stages", "Percentage of time spent in GC", run.map(_ => "success")),
        healthRow("Garbage Collection", "Percentage of time spent in GC", run.map(_ => "success"))
      )
    )


  def runShuffle(run: Option[FlowExecution]): ReactElement = {
    val metrics = run.flatMap(_.metrics.map(_.metrics))
    Fragment(
      div(h6("Shuffle")),
      ul(className := "media-list media-list-linked pb-15")(
        row("Read", Try(s"${metrics.get(ShuffleReadBytesRead).value} / 1024} MB / ${metrics.get(ShuffleReadRecordsRead).value} records").toOption),
        row("Write", Try(s"${metrics.get(ShuffleWriteBytesWritten).value} / 1024} MB / ${metrics.get(ShuffleWriteRecordsWritten).value} records").toOption),
      )
    )
  }


  def runResources(run: Option[FlowExecution]): ReactElement =
    Fragment(
      div(h6("Resources")),
      ul(className := "media-list media-list-linked pb-15")(
        row("Executors", Try(s"${run.get.executorCount.get} / 30").toOption),
        row("Cores", Try(s"${run.get.coresPerExecutor.get} per executor / ${run.get.coresPerExecutor.get * run.get.executorCount.get } total").toOption),
        row("Memory", Try(s"${run.get.metrics.get.metrics(AggregateMetric.PeakExecutionMemory).mean.toString} MB").toOption)
      )
    )


  private def row(title: String, value: Option[String]) = {
    val str = value.getOrElse("Not available")
    li(key := title, className := "media")(
      div(className := "media-body")(
        span(className := "media-heading text-semi-bold")(title),
        span(className := "media-annotation")(str)
      )
    )
  }

  private def healthRow(title: String, description: String, status: Option[String]) =
    li(key := title, className := "media")(
      div(className := "media-body")(
        span(className := "media-heading text-semi-bold")(title),
        span(className := "media-annotation")(description)
      ),
      div(className := "media-right media-middle")(span(className := s"status-mark bg-${status.getOrElse("grey-300")}"))
    )
}