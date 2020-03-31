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

import org.junit.Test
import org.junit.Assert.{assertEquals, assertTrue}
import com.typesafe.scalalogging.Logger

class Log4j2ControllerTest {
  @Test
  def main(): Unit = {
    val backend = LoggingBackend()
    Logger("kafka.controller.Foo").debug("hello")
    Logger("kafka.Bar").debug("hello")
    assertEquals(Map("root" -> "INFO",
      "kafka.controller" -> "TRACE",
      "kafka.controller.Foo" -> "TRACE",
      "kafka" -> "INFO",
      "kafka.Bar" -> "INFO"), backend.loggers)
    // So loggers are created by Logger(), and the logger configs for those loggers exist
    // but there is no accessible logger config for other "loggers" defined in the config file.
    assertEquals("INFO", backend.existingLoggerLevel("kafka"))
    assertEquals("TRACE", backend.existingLoggerLevel("kafka.controller"))
    assertEquals("TRACE", backend.existingLoggerLevel("kafka.controller.Foo"))
    assertEquals("INFO", backend.existingLoggerLevel("kafka.Bar"))

    // Changing the level of a logger that was defined in the config file
    // does not change the level of any existing child loggers...
    assertTrue(backend.logLevel("kafka", "DEBUG"))
    assertEquals(Map("root" -> "INFO",
      "kafka.controller" -> "TRACE",
      "kafka.controller.Foo" -> "TRACE",
      "kafka" -> "DEBUG",
      "kafka.Bar" -> "INFO"), backend.loggers)
    assertEquals("DEBUG", backend.existingLoggerLevel("kafka"))
    assertEquals("TRACE", backend.existingLoggerLevel("kafka.controller"))
    assertEquals("TRACE", backend.existingLoggerLevel("kafka.controller.Foo"))
    assertEquals("INFO", backend.existingLoggerLevel("kafka.Bar"))
    // ...but is used for new loggers
    Logger("kafka.Baz").debug("hello")
    assertEquals("DEBUG", backend.existingLoggerLevel("kafka.Baz"))

    // unsetting the log level of an existing logger means it picks up the level from its parent
    assertTrue(backend.unsetLogLevel("kafka.Bar"))
    assertEquals("DEBUG", backend.existingLoggerLevel("kafka.Bar"))
    assertEquals(Map("root" -> "INFO",
      "kafka.controller" -> "TRACE",
      "kafka.controller.Foo" -> "TRACE",
      "kafka" -> "DEBUG",
      "kafka.Bar" -> "DEBUG",
      "kafka.Baz" -> "DEBUG"), backend.loggers)
  }
}
