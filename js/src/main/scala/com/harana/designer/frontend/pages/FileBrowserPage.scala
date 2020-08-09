package com.harana.designer.frontend.pages

import com.harana.ui.components.elements.Page
import com.harana.ui.components.{ColumnSize, Device}
import com.harana.ui.components.panels.LoginPanel
import com.harana.ui.components.sidebar._
import com.harana.ui.components.table.{Column, GroupedTable, Row, RowGroup}
import com.harana.ui.external.datatable.{Datatable, TableHeader}
import slinky.core.StatelessComponent
import slinky.core.annotations.react
import slinky.web.html._

import scala.scalajs.js
import scala.scalajs.js.JSON

@react class FileBrowserPage extends StatelessComponent {

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

    val textItems = List(
      TextItem("Description", "Predicts the percentage takeup for RSM"),
      TextItem("Last Updated", "2 weeks ago")
    )
    Sidebar(List(
      SidebarSection(Some("About"), List(TextSection(textItems))),
      SidebarSection(Some("Search"), List(SearchSection())),
      SidebarSection(Some("Categories"), List(NavigationCategory(List(NavigationSection(navigationItems)))))
    ))
  }

  def pageContent = {
    val columns = List(
      Column("Model", Map(Device.Tablet -> ColumnSize.Three)),
      Column("Team", Map(Device.Tablet -> ColumnSize.One)),
      Column("Accuracy", Map(Device.Tablet -> ColumnSize.One)),
      Column("Revenue Saved", Map(Device.Tablet -> ColumnSize.One)),
      Column("Status", Map(Device.Tablet -> ColumnSize.One)),
    )

    val yesterdayRows = List(
      Row(),
      Row(),
      Row(),
      Row(),
      Row(),
      Row()
    )

    val rowGroups = List(
      RowGroup("Executed Yesterday", yesterdayRows),
      RowGroup("Executed Today", yesterdayRows)
    )
    GroupedTable(columns, rowGroups)
  }

  def render =
    Page("RSM Forecast", "RSM Forecast", Some(Common.navigationDropdown), List(), Some(Common.navigationBar(Common.projectsItem)), sidebar = Some(sidebar), content = pageContent)
}