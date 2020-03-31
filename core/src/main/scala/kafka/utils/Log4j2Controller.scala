/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.utils

import java.net.URI
import java.util.Locale

import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.config.properties.PropertiesConfigurationBuilder
import org.apache.logging.log4j.core.config.{Configurator, LoggerConfig, Loggers}
import org.apache.logging.log4j.{Level, LogManager, Logger}

import scala.collection.JavaConverters._
import scala.collection.mutable

object Log4j2Controller extends LoggingBackend {
  override def name: String = "log4j2"
  /**
    * Returns a map of the log4j2 loggers and their assigned log level.
    * If a logger does not have a log level assigned, we return the root logger's log level
    */
  def loggers: mutable.Map[String, String] = {
    val logs = new mutable.HashMap[String, String]()
    val rootLoggerLvl = existingLoggerLevel(LoggingBackend.ROOT_LOGGER)
    // Log4j2 separates the concept of logger config from logger. In log4j1 they were effectively the same thing.
    // This means that to have a compatible API to log4j1 we need to pretend there's no difference.
    // Thus add all the loggers **and log configs** to the map, since both are addressable via #loglevel()
    val loggers = context.getLoggers
    for (logger <- loggers.asScala) {
      val config = context.getConfiguration.getLoggerConfig(logger.getName)
      val level = if (config.getLevel != null) config.getLevel.toString else rootLoggerLvl
      logs.put(config.getName, level)
    }
    for (logger <- loggers.asScala) {
      val level = if (logger.getLevel != null) logger.getLevel.toString else rootLoggerLvl
      logs.put(logger.getName, level)
    }
    // Remove the empty logger (which is what our API calls the ROOT_LOGGER)
    logs.put(LoggingBackend.ROOT_LOGGER, rootLoggerLvl)
    logs.remove("")
    logs
  }

  /**
    * Sets the log level of a particular logger
    */
  def logLevel(loggerName: String, logLevel: String): Boolean = {
    val level = Level.getLevel(logLevel.toUpperCase(Locale.ROOT))
    loggerOrConfig(loggerName) match {
      case None => false
      case Some(Left(_)) => {
        Configurator.setLevel(loggerName, level)
        true
      }
      case Some(Right(config)) => {
        config.setLevel(level)
//        context.updateLoggers()
//        context.getConfiguration.getLoggerConfig()
//        context.getConfigLocation
//        new PropertiesConfigurationBuilder().build().
//        Configurator.reconfigure()
//        context.reconfigure()
//        context.updateLoggers()
        true
      }
    }
//    val log = existingLoggerLevel(loggerName)
//    if (!loggerName.trim.isEmpty
//      && !logLevel.trim.isEmpty
//      && log != null) {
//      Configurator.setLevel(loggerName, Level.getLevel(logLevel.toUpperCase(Locale.ROOT)))
//      true
//    }
//    // TODO set the level on the config
//    else false
  }

  /**
   * Clears the log level of a particular logger
   */
  def unsetLogLevel(loggerName: String): Boolean = {
    loggerOrConfig(loggerName) match {
      case None => false
      case Some(Left(_)) => {
        Configurator.setLevel(loggerName, null)
        true
      }
      case Some(Right(config)) => {
        config.setLevel(null)
        true
      }
    }
//    val log = existingLoggerLevel(loggerName)
//    if (!loggerName.trim.isEmpty && log != null) {
//      Configurator.setLevel(loggerName, null)
//      true
//    }
//      // TODO set the level on the config
//    else false
  }

  def loggerExists(loggerName: String): Boolean = existingLoggerLevel(loggerName) != null

  private def context = LogManager.getContext(false).asInstanceOf[LoggerContext]

  def loggerOrConfig(name: String): Option[Either[Logger, LoggerConfig]] = {
    if (name == LoggingBackend.ROOT_LOGGER) {
      Some(Left(LogManager.getRootLogger()))
    } else if (context.hasLogger(name)) {
      Some(Left(LogManager.getLogger(name)))
    } else {
      val value = context.getConfiguration.getLoggerConfig(name)
      if (value.getName == name)
        Some(Right(value))
      else
        None
    }
  }

  def existingLoggerLevel(loggerName: String): String = {
    loggerOrConfig(loggerName) match {
      case None => null
      case Some(Left(logger)) => logger.getLevel.toString
      case Some(Right(config)) => config.getLevel.toString
    }
//    if (loggerName == LoggingBackend.ROOT_LOGGER)
//      LogManager.getRootLogger().getLevel.toString
//    else if (context.hasLogger(loggerName)) {
//      val name1: Logger = LogManager.getLogger(loggerName)
//      name1.getLevel.toString
//    }
//      // TODO else try a logger config
//    else {
//      val map = context.getConfiguration.getLoggers.values.asScala.map(lc => lc.getName -> lc).toMap
//      map.get(loggerName).map(lc => lc.getLevel.toString).get
//    }
  }

}