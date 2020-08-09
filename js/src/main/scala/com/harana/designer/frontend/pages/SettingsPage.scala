package com.harana.designer.frontend.pages

import com.harana.ui.components.elements.Page
import slinky.core.StatelessComponent
import slinky.core.annotations.react
import slinky.web.html.h1

@react class SettingsPage extends StatelessComponent {

  type Props = Unit

  def pageContent = h1("Test")

  def render =
    Page("Settings", "Settings", Some(Common.navigationDropdown), List(), Some(Common.navigationBar(Common.appsItem)), content = pageContent)
}