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
package org.apache.twill.internal.appmaster;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggerContextListener;

/**
 *
 */
abstract class LoggerContextListenerAdapter implements LoggerContextListener {

  private final boolean resetResistant;

  protected LoggerContextListenerAdapter(boolean resetResistant) {
    this.resetResistant = resetResistant;
  }

  @Override
  public final boolean isResetResistant() {
    return resetResistant;
  }

  @Override
  public void onStart(LoggerContext context) {
  }

  @Override
  public void onReset(LoggerContext context) {
  }

  @Override
  public void onStop(LoggerContext context) {
  }

  @Override
  public void onLevelChange(Logger logger, Level level) {
  }
}
