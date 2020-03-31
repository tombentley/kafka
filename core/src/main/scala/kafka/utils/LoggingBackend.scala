/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import org.slf4j.impl.StaticLoggerBinder

import scala.collection.mutable

/**
 * A façade for a logging backend (which could be either log4j or logj42).
 */
trait LoggingBackend {
  /**
   * @return The name of the backend.
   */
  def name: String
  /**
   * Returns a map of logger names and their assigned log level.
   * If a logger does not have a log level assigned, we return the root logger's log level
   */
  def loggers: mutable.Map[String, String]
  /**
   * Sets the log level of a particular logger.
   * If the logger does not already exist it is not created.
   * @return true if the level was set successfully.
   */
  def logLevel(loggerName: String, logLevel: String): Boolean
  /**
   * Clears/unsets the log level of a particular logger. The new level will inherited from a parent logger.
   * @return true if the level was unset successfully.
   */
  def unsetLogLevel(loggerName: String): Boolean

  /**
   * Tests whether a logger with the given name exists.
   * @param loggerName The logger name.
   * @return true if the logger exists.
   */
  def loggerExists(loggerName: String): Boolean

  /**
   * Return the effective level of the given logger, or null if it does not exist.
   * @param loggerName
   * @return
   */
  def existingLoggerLevel(loggerName: String): String
}

object LoggingBackend {
  val ROOT_LOGGER = "root"

  /**
   * Gets the logging backend façade.
   */
  def apply(): LoggingBackend = {
    // static logger binder mechanism for slf4j up to 1.7.x
    val binder = StaticLoggerBinder.getSingleton
    binder.getLoggerFactoryClassStr match {
      case "org.slf4j.impl.Log4jLoggerFactory" => Log4j1Controller
      case "org.apache.logging.slf4j.Log4jLoggerFactory" => Log4j2Controller
      case other => throw new RuntimeException(s"Unknown logger class factory $other")
    }
  }
}