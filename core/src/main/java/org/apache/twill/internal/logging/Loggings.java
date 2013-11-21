/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.internal.logging;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;

/**
 *
 */
public final class Loggings {

  public static void forceFlush() {
    ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();

    if (loggerFactory instanceof LoggerContext) {
      Appender<ILoggingEvent> appender = ((LoggerContext) loggerFactory).getLogger(Logger.ROOT_LOGGER_NAME)
                                                                        .getAppender("KAFKA");
      if (appender != null && appender instanceof KafkaAppender) {
        ((KafkaAppender) appender).forceFlush();
      }
    }
  }

  private Loggings() {
  }
}
