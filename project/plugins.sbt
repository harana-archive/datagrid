credentials += Credentials("GitHub Package Registry", "maven.pkg.github.com", "harana-bot", "ca7b2504bf025a44e15488249626aaea9dafcccc")
resolvers += "Harana" at "https://maven.pkg.github.com/harana"
addSbtPlugin("sbt-plugin" % "sbt-js_jvm" % "1.3.5")
addSbtPlugin("io.github.cquiroz" % "sbt-tzdb" % "0.3.1")