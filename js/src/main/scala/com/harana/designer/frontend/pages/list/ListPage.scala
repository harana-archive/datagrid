package com.harana.designer.frontend.pages.list

import com.harana.designer.frontend.pages.Common
import com.harana.ui.components.elements._
import com.harana.ui.components.sidebar._
import com.harana.ui.components.structure.Grid
import com.harana.ui.components.widgets.{CatalogChartType, CatalogWidget}
import com.harana.ui.components.{ColumnSize, LinkType, Ref}
import slinky.core.{Component, StatelessComponent}
import slinky.core.annotations.react

import scala.collection.mutable.ListBuffer

@react class ListPage extends Component {

  case class Props(pageTitle: String,
                   headingTitle: String,
                   headingTitleMenu: Option[Menu.Props] = None,
                   headingItems: List[HeadingItem] = List(),
                   headerNavigationBar: Option[Ref[NavigationBar]] = None,
                   headerFixedNavigationBar: Boolean = true,
                   footerNavigationBar: Option[Ref[NavigationBar]] = None,
                   allowSearch: Boolean = true,
                   allowCategories: Boolean = true,
                   categories: List[Category] = List(),
                   items: (String, Category) => List[Item])

  override def initialState = State(null, "")
  case class State(category: Category, search: String)

  def sidebar = {

    val navigationItems = props.categories.map { item =>
      NavigationSectionItem("", onClick = () => this.setState(s => s.copy(category = item)), icon = item.icon, rightText = item.rightText)
    }

    val sidebarSections = ListBuffer[SidebarSection]()
    if (props.allowSearch) sidebarSections += SidebarSection(Some("Search"), List(SearchSection()))
    if (props.allowCategories) sidebarSections += SidebarSection(Some("Categories"), List(NavigationCategory(List(NavigationSection(navigationItems)))))
    Sidebar(sidebarSections.toList)
  }

  def pageContent =
    Grid(
      props.items(state.search, state.category).map { item =>
        CatalogWidget(item.title, item.subtitle, item.rightText, item.chartType, item.link, item.color)
      },
      ColumnSize.Three
    )

  def render =
    Page(props.pageTitle, props.headingTitle, None, List(), Some(Common.navigationBar(Common.accountItem)), sidebar = Some(sidebar), content = pageContent)

}