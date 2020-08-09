package com.harana.designer.frontend.pages

import com.harana.ui.components.elements.{Icon, Page}
import com.harana.ui.components.sidebar._
import com.harana.ui.external.ace_editor.{AceEditor, ReactAce, ReactAcePython}
import com.harana.ui.external.split_pane.SplitPane
import com.harana.ui.external.syntax_highlighter.{HighlightStyle, SyntaxHighlighter}
import slinky.core.StatelessComponent
import slinky.core.annotations.react

@react class SnippetsPage extends StatelessComponent {

  type Props = Unit

  // Load Python syntax highlighting
  ReactAce
  ReactAcePython

  val snippets = List(
    TextListItem("Snippet One", "This is some sample text. "),
    TextListItem("Snippet Two", "This is some sample text. "),
    TextListItem("Snippet Three", "This is some sample text. "),
    TextListItem("Snippet Four", "This is some sample text. "),
    TextListItem("Snippet Five", "This is some sample text. "),
    TextListItem("Snippet Six", "This is some sample text. "),
    TextListItem("Snippet Seven", "This is some sample text. "),
    TextListItem("Snippet Eight", "This is some sample text. "),
    TextListItem("Snippet Nine", "This is some sample text. "),
    TextListItem("Snippet Ten", "This is some sample text. ")
  )

  def sidebarTabs = {
    val meCategory = SidebarSection(None, TextListSection(snippets))
    val teamCategory = SidebarSection(None, TextListSection(snippets))
    val communityCategory = SidebarSection(None, TextListSection(snippets))

    List(
      Tab("me", List(meCategory), Some(Icon.User), active = true),
      Tab("team", List(teamCategory), Some(Icon.Users)),
      Tab("community", List(communityCategory), Some(Icon.Earth))
    )
  }

  def headerIcons = List(
    //HeadingIcon("icon-newspaper", LinkType.OpenDropdown, Some(Common.accountDropdown))
  )

  def sidebar =
    Sidebar(List(), sidebarTabs, separateCategories = false)

  def pageContent = {
    SplitPane(split = "horizontal", minSize = 300)(
      AceEditor(width = "100%", height = "100%", showPrintMargin = false, mode = "python"),
      SyntaxHighlighter(language = "scala", style = HighlightStyle.tomorrow)(
        """|[info] Compiling 1 Scala source to /Users/naden/Developer/harana/harana/js/target/scala-2.12/classes ...
           |[info] Done compiling.
           |[info] Fast optimizing /Users/naden/Developer/harana/harana/js/target/scala-2.12/scalajs-bundler/main/harana-fastopt.js
           |[info] Writing module entry point for harana-fastopt-entrypoint.js
           |[info] Building webpack library bundles for harana-fastopt in /Users/naden/Developer/harana/harana/js/target/streams/_global/_global/_global/streams/fastOptJS-webpack-libraries
           |[info] Writing scalajs.webpack.config.js
           |[info] Version: 4.37.0
           |[info] Hash: f24123a427726e5f6d6f
           |[info] Time: 3238ms
           |[info] Path: /Users/naden/Developer/harana/harana/js/target/scala-2.12/scalajs-bundler/main
           |[info] Built at 2019-12-15T15:37:11.660
           |""".stripMargin
      )
    )
  }

  def render =
    Page("Python", "Python", Some(Common.navigationDropdown), headerIcons, Some(Common.navigationBar(Common.snippetsItem)), sidebar = Some(sidebar), content = pageContent)
}