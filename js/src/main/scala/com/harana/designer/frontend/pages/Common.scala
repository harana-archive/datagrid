package com.harana.designer.frontend.pages

import com.harana.ui.components.literal
import com.harana.ui.components.elements._
import com.harana.ui.components.LinkType

object Common {

  val projectsItem = NavigationBarItem(Some("Projects"), Some(ItemType.Icon(Icon.Folder), ItemPosition.Left))
  val flowsItem = NavigationBarItem(Some("Flows"), Some(ItemType.Icon(Icon.Design), ItemPosition.Left))
  val appsItem = NavigationBarItem(Some("Apps"), Some(ItemType.Icon(Icon.FileText2), ItemPosition.Left))
  val snippetsItem = NavigationBarItem(Some("Snippets"), Some(ItemType.Icon(Icon.Scissors), ItemPosition.Left))
  val dataItem = NavigationBarItem(Some("Data"), Some(ItemType.Icon(Icon.Database), ItemPosition.Left))
  val catalogsItem = NavigationBarItem(Some("Catalogs"), Some(ItemType.Icon(Icon.Archive), ItemPosition.Left))
  val storiesItem = NavigationBarItem(Some("Stories"), Some(ItemType.Icon(Icon.Magazine), ItemPosition.Left))
  val storeItem = NavigationBarItem(Some("Store"), Some(ItemType.Icon(Icon.Bag), ItemPosition.Left))
  val modelsItem = NavigationBarItem(Some("Models"), Some(ItemType.Icon(Icon.Cube2), ItemPosition.Left))
  val servicesItem = NavigationBarItem(Some("Services"), Some(ItemType.Icon(Icon.Upload10), ItemPosition.Left))
  val schedulesItem = NavigationBarItem(Some("Schedules"), Some(ItemType.Icon(Icon.Calendar3), ItemPosition.Left))
  val containersItem = NavigationBarItem(Some("Containers"), Some(ItemType.Icon(Icon.Cube2), ItemPosition.Left))
  val referralItem = NavigationBarItem(None, Some(ItemType.Icon(Icon.Trophy4), ItemPosition.Left))

  def accountDropdown = {
    List(
      TextMenuItem("Profile"),
      TextMenuItem("Settings"),
      DividerMenuItem(),
      TextMenuItem("Feedback"),
      TextMenuItem("Support"),
      DividerMenuItem(),
      TextMenuItem("Sign out")
    )
  }

  val accountItem = NavigationBarItem(None, None, Some("Chloe"))

  val leftNavigationItems = List(
    flowsItem -> LinkType.Page("/flows"),
    appsItem -> LinkType.Page("/apps/jupyter"),
    snippetsItem -> LinkType.Page("/snippets"),
    dataItem -> LinkType.Page("/data"))

  val rightNavigationItems = List(
    referralItem -> LinkType.Url(""),
    accountItem -> LinkType.Menu(Menu.Props(accountDropdown))
  )

  def navigationDropdown = {
    val item1 = TextMenuItem("One")
    val item2 = TextMenuItem("Two")
    Menu.Props(List(item1, item2), style = Some(literal("top" -> "80%")))
  }

  def navigationBar(activeItem: NavigationBarItem) = {
    NavigationBar(leftNavigationItems, rightNavigationItems, activeItem, None, None, List(NavigationBarStyle.Inverse, NavigationBarStyle.Transparent), "/public/images/logo.png", LinkType.Url("http://harana.com"))
  }
}