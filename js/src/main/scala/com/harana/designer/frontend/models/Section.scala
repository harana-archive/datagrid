package com.harana.designer.frontend.models

sealed trait Section
object Section {
  case object Data extends Section
  case object Flows extends Section
  case object Home extends Section
  case object Projects extends Section
  case object Settings extends Section
  case object Snippets extends Section
}