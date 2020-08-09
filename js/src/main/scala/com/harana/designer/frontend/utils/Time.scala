package com.harana.designer.frontend.utils

import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant, ZoneId}


object Time {

  val formatter = DateTimeFormatter.ofPattern("d LLL YYYY - hh:mm:ss a", java.util.Locale.ENGLISH).withZone(ZoneId.of("Australia/Sydney"))

  def format(instant: Instant): String = {
    formatter.format(instant)
  }

  def pretty(duration: Duration): String = {
    if (duration.toDays > 0) return s"${duration.toDays} days"
    if (duration.toHours > 0) return s"${duration.toHours} hours"
    if (duration.toMinutes > 0) return s"${duration.toMinutes} mins"
    if (duration.getSeconds > 0) return s"${duration.getSeconds} secs"
    if (duration.toMillis > 0) return s"${duration.toMillis} msecs"
    ""
  }

  def pretty(instant1: Instant, instant2: Instant): String = {
    pretty(Duration.between(instant1, instant2))
  }

  def pretty(instant: Instant): String = {
    pretty(instant, Instant.now())
  }
}
