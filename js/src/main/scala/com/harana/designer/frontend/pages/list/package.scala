package com.harana.designer.frontend.pages

import com.harana.ui.components.LinkType
import com.harana.ui.components.elements.Color
import com.harana.ui.components.widgets.CatalogChartType

package object list {

  case class Category(name: String, icon: Option[String], rightText: Option[String])

  case class Item(title: String, subtitle: Option[String], rightText: Option[String], chartType: CatalogChartType, link: LinkType, color: Color)

}
