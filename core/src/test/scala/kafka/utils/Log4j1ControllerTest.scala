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

import java.util.Properties

import com.typesafe.scalalogging.Logger
import org.apache.log4j.{AppenderSkeleton, Hierarchy, LogManager, PropertyConfigurator}
import org.apache.log4j.spi.{Configurator, LoggingEvent}
import org.junit.Test
import org.junit.Assert.{assertEquals, assertTrue}

import scala.collection.mutable.ListBuffer

case class Evt(logger: String, level: String)

object TestAppender {
  val events = ListBuffer[Evt]()
}
class TestAppender extends AppenderSkeleton {

  override def append(event: LoggingEvent): Unit = {
    TestAppender.events += Evt(event.getLoggerName, event.getLevel.toString)
  }

  override def close(): Unit = {}

  override def requiresLayout(): Boolean = false
}
class Log4j1ControllerTest {



  @Test
  def test1() = {
    LogManager.shutdown()
    val h = LogManager.getLoggerRepository.asInstanceOf[Hierarchy]
    h.clear()
    val p = new Properties()
    p.setProperty("log4j.appender.test", "kafka.utils.TestAppender")
    p.setProperty("log4j.rootLogger", "INFO, test")
    p.setProperty("log4j.logger.a", "INFO")
    p.setProperty("log4j.logger.a.b", "DEBUG")
    p.setProperty("log4j.logger.x", "WARN")
    PropertyConfigurator.configure(p)
    Logger("a").info("hello")
    Logger("a").debug("hello")
    Logger("a.b").debug("hello")
    Logger("a.b").trace("hello")
    Logger("a.b.C").debug("hello")
    Logger("a.b.C").trace("hello")
    Logger("x").info("hello")
    Logger("x").warn("hello")
    Logger("z").info("hello")
    Logger("z").debug("hello")
    assertEquals(List(Evt("a", "INFO"), Evt("a.b", "DEBUG"), Evt("a.b.C", "DEBUG"), Evt("x", "WARN"), Evt("z", "INFO")), TestAppender.events)

    assertEquals(Map("root" -> "INFO", "z" -> "INFO", "a" -> "INFO", "a.b" -> "DEBUG", "a.b.C" -> "DEBUG"), Log4j1Controller.loggers.toString())
  }

}
