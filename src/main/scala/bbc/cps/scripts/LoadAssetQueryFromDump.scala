package main.scala.bbc.cps.scripts

import scala.io.Source

case class Event(eventType: String,
                 userId: String,
                 eventCreated: String,
                 assetId: String,
                 majorVersion: Int = 0,
                 minorVersion: Int = 0)

case class AssetData(assetId: String,
                     userId: String,
                     updated: String,
                     created: String,
                     publishStatus: String,
                     hasBeenPublished: String,
                     lastPublished: String)

object Main extends App {
  val dumpFile = "/dumps/db-dump_int_asset_branch_query.csv"
  val assetIdFile = //TODO
  val testFile = "/test.csv"
  val otherStatuses = List("withdrawn", "deleted")

  val stream = getClass.getResourceAsStream(dumpFile)
  for (line <- Source.fromInputStream(stream).getLines()) {
    val Array(assetId, userId, created, updated, publishStatus, hasBeenPublished, lastPublished) = line.split(",").map(_.trim)
    val assetData = AssetData(assetId, userId, created, updated, publishStatus, stripDoubleQuotes(hasBeenPublished), lastPublished)

    val events:Array[Event] = stripDoubleQuotes(publishStatus) match {
      case "draft" => Array(createEvent(assetData))
      case "published" => Array(createPublished(assetData))
      case otherStatus if otherStatuses.contains(otherStatus) =>
        if (assetData.hasBeenPublished.toBoolean) {
          Array(createEvent(assetData), createPublished(assetData) , createOtherStatuses(assetData))
        }
        else {
          Array(createEvent(assetData), createOtherStatuses(assetData))
        }
      case unhandledStatus =>
        println(s"COULD NOT HANDLE status $unhandledStatus for [$assetData]")
        Array.empty[Event]
    }

    events.map(_ => composeSql(_))
  }

  def stripDoubleQuotes(text: String) = text.replaceAll("^\"" ,"").replaceAll("\"$" ,"")

  def addDoubleQuotes(text: String) = s""""$text""""

  def composeSql(event: Event): String = {
    val cols = Array("event_type", "user_id", "event_created", "asset_id", "major_version", "minor_version")
    val values = Array(addDoubleQuotes(event.eventType), event.userId, event.eventCreated, event.assetId, event.majorVersion, event.minorVersion)
    s"INSERT INTO events (${cols.mkString(",")}) VALUES (${values.mkString(",")});"
  }

  def createEvent(assetData: AssetData): Event = {
    Event(
      eventType = "CREATED",
      userId = assetData.userId,
      eventCreated = assetData.created,
      assetId = assetData.assetId
    )
  }

  def createPublished(assetData: AssetData) = {
    Event(
      eventType = "PUBLISHED",
      userId = assetData.userId,
      eventCreated = assetData.lastPublished,
      assetId = assetData.assetId,
      majorVersion = 1
    )
  }

  def createOtherStatuses(assetData: AssetData) = {
    Event(
      eventType = assetData.publishStatus.toUpperCase,
      userId = assetData.userId,
      eventCreated = assetData.updated,
      assetId = assetData.assetId,
      majorVersion = if (assetData.hasBeenPublished.toBoolean) 1 else 0
    )
  }
}