package com.harana.designer.frontend.pages

import com.harana.ui.components.elements.{Color, Page}
import com.harana.ui.components.sidebar._
import com.harana.ui.components.structure.Grid
import com.harana.ui.components.widgets.{CatalogChartType, CatalogWidget}
import com.harana.ui.components.{ColumnSize, LinkType}
import slinky.core.StatelessComponent
import slinky.core.annotations.react
import slinky.core.facade.ReactElement

@react class DataSourcesPage extends StatelessComponent {

  type Props = Unit

  def sidebar = {
    val navigationItems = List(
      (" Me", "icon-user"),
      ("Team", "icon-users"),
      (" Organisation", "icon-office"),
      (" Community", "icon-earth")
    ).map { item =>
      val fn = () => print("Hi")
      NavigationSectionItem(item._1, onClick = fn, icon = Some(item._2), rightText = Some("5"))
    }

    Sidebar(List(
      SidebarSection(Some("Search"), List(SearchSection())),
      SidebarSection(Some("Categories"), List(NavigationCategory(List(NavigationSection(navigationItems)))))
    ))
  }

  def pageContent = {

    val gridItems = List[ReactElement](
      CatalogWidget("My Cloud", Some("Harana"), Some("32"), CatalogChartType.Sparkline, LinkType.Page("harana-cloud"), Color.Red300),
      CatalogWidget("Team Cloud", Some("Harana"), Some("32"), CatalogChartType.Sparkline, LinkType.Page("harana-cloud"), Color.Pink300),
      CatalogWidget("Company Cloud", Some("Harana"), Some("32"), CatalogChartType.Sparkline, LinkType.Page("harana-cloud"), Color.Green300),
      CatalogWidget("Xero", Some("Finance"), Some("32"), CatalogChartType.Bar, LinkType.Url("http://apple.com"), Color.Slate300),
      CatalogWidget("Oracle ERP", Some("Finance"), Some("32"), CatalogChartType.Bar, LinkType.Url("http://apple.com"), Color.Slate300),
      CatalogWidget("Salesforce", Some("Marketing"), Some("32"), CatalogChartType.Bar, LinkType.Url("http://apple.com"), Color.Slate300),
      CatalogWidget("Netezza EDW", Some("CDO"), Some("32"), CatalogChartType.Bar, LinkType.Url("http://apple.com"), Color.Slate300),
      CatalogWidget("AWS S3", Some("CDO"), Some("32"), CatalogChartType.Sparkline, LinkType.Url("http://apple.com"), Color.Slate300),
      CatalogWidget("ML Models", Some("CDO"), Some("32"), CatalogChartType.Line, LinkType.Url("http://apple.com"), Color.Slate300)
    )

    Grid(gridItems, ColumnSize.Three)
  }

  def render =
    Page("Data", "Data", None, List(), Some(Common.navigationBar(Common.accountItem)), sidebar = Some(sidebar), content = pageContent)
}