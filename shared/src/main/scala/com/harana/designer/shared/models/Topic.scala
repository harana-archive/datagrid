package com.harana.shared.models

sealed abstract class Topic(val name: String) {
  override def toString: String = name
}

object Topic {
  case object Project

  case object FlowStart extends Topic("flow.start")
  case object FlowStop extends Topic("flow.stop")
  case object FlowSuccess extends Topic("flow.success")
  case object FlowFail extends Topic("flow.fail")

  case object ActionStart extends Topic("action.start")
  case object ActionSuccess extends Topic("action.success")
  case object ActionFail extends Topic("access.fail")

  case object SnippetStart extends Topic("snippet.start")
  case object SnippetSuccess extends Topic("snippet.success")
  case object SnippetFail extends Topic("snippet.fail")

  case object UserLogin extends Topic("user.login")
  case object UserLogout extends Topic("user.logout")

  case object EntityInit extends Topic("entity.init")
  case object EntityLatest extends Topic("entity.latest")
  case object EntityCreate extends Topic("entity.create")
  case object EntityDelete extends Topic("entity.delete")
  case object EntityUpdate extends Topic("entity.update")
  case object EntityFilter extends Topic("entity.filter")
  case object EntitySearch extends Topic("entity.search")

  case object PlanCancel extends Topic("plan.cancel")
  case object PlanChange extends Topic("plan.change")

  case object SparkAppStart extends Topic("spark.app.start")
  case object SparkAppEnd extends Topic("spark.app.end")
  case object SparkExecutorAdd extends Topic("spark.executor.add")
  case object SparkExecutorRemove extends Topic("spark.executor.remove")
  case object SparkStageStart extends Topic("spark.stage.start")
  case object SparkStageEnd extends Topic("spark.stage.end")
  case object SparkTaskStart extends Topic("spark.task.start")
  case object SparkTaskEnd extends Topic("spark.task.end")
  case object SparkTaskResult extends Topic("spark.task.result")
}