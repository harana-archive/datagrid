package com.harana.shared.models

sealed trait HttpMethod
case object GET extends HttpMethod
case object POST extends HttpMethod
case object PUT extends HttpMethod
case object DELETE extends HttpMethod
case object PATCH extends HttpMethod

sealed abstract class Entity(val name: String)
case object Flow extends Entity("flow")

sealed abstract class Route(val name: String, val method: HttpMethod) {
  override def toString: String = s"${method.toString}: $name"
}

object Route {
  case class FlowStart(id: String) extends Route(s"/flow/$id/start", POST)
  case class FlowStop(id: String) extends Route(s"/flow/$id/stop", POST)

  case object UserLogin extends Route("/login", POST)
  case object UserLogout extends Route("/logout", POST)

  case class EntityList(entity: Entity) extends Route(s"/${entity.name}", GET)
  case class EntityListLatest(entity: Entity) extends Route(s"/${entity.name}/latest", GET)
  case class EntityCreate(entity: Entity) extends Route(s"/${entity.name}", POST)
  case class EntityDelete(entity: Entity) extends Route(s"/${entity.name}", DELETE)
  case class EntityGet(entity: Entity, id: String) extends Route(s"/${entity.name}/$id", GET)
  case class EntityUpdate(entity: Entity) extends Route(s"/${entity.name}", PUT)
  case class EntityFilter(entity: Entity) extends Route(s"/${entity.name}/filter", GET)
  case class EntitySearch(entity: Entity) extends Route(s"/${entity.name}/search", GET)

  case object SettingsPlanCancel extends Route("/settings/plan/cancel", POST)
  case object SettingsPlanUpdate extends Route("/settings/plan", PUT)
}