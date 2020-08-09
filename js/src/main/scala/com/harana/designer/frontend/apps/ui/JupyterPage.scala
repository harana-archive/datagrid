package com.harana.designer.frontend.apps.ui

import com.harana.designer.frontend.pages.Common
import com.harana.ui.components.elements.Page
import slinky.core.FunctionalComponent
import slinky.core.annotations.react
import slinky.web.html._

@react object JupyterPage {
  type Props = Unit

  def pageContent = {
    iframe(src := "http://localhost:8888/lab", className := "frame")
  }

  val component = FunctionalComponent[Unit] { _ =>
    Page("Jupyter", "Jupyter", Some(Common.navigationDropdown), List(), Some(Common.navigationBar(Common.snippetsItem)), content = pageContent, fixedSizeContent = true, padding = false)
  }
}