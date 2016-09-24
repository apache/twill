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
package org.apache.twill.internal;

import org.apache.twill.api.TwillRuntimeSpecification;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.LogEntry;

import java.util.Map;

/**
 *
 */
public class DefaultTwillRuntimeSpecification implements TwillRuntimeSpecification {

  private final TwillSpecification twillSpecification;

  private final String fsUser;
  private final String twillAppDir;
  private final String zkConnectStr;
  private final String twillRunId;
  private final String twillAppName;
  private final String reservedMemory;
  private final String rmSchedulerAddr;
  private final String logLevel;
  private final Map<String, Map<String, LogEntry.Level>> logLevelArguments;

  public DefaultTwillRuntimeSpecification(TwillSpecification twillSpecification, String fsUser, String twillAppDir,
                                          String zkConnectStr, String twillRunId, String twillAppName,
                                          String reservedMemory, String rmSchedulerAddr, String logLevel,
                                          Map<String, Map<String, LogEntry.Level>> logLevelArguments) {
    this.twillSpecification = twillSpecification;
    this.fsUser = fsUser;
    this.twillAppDir = twillAppDir;
    this.zkConnectStr = zkConnectStr;
    this.twillRunId = twillRunId;
    this.twillAppName = twillAppName;
    this.reservedMemory = reservedMemory;
    this.rmSchedulerAddr = rmSchedulerAddr;
    this.logLevel = logLevel;
    this.logLevelArguments = logLevelArguments;
  }

  @Override
  public TwillSpecification getTwillSpecification() {
    return twillSpecification;
  }

  @Override
  public String getFsUser() {
    return fsUser;
  }

  @Override
  public String getTwillAppDir() {
    return twillAppDir;
  }

  @Override
  public String getZkConnectStr() {
    return zkConnectStr;
  }

  @Override
  public String getTwillRunId() {
    return twillRunId;
  }

  @Override
  public String getTwillAppName() {
    return twillAppName;
  }

  @Override
  public String getReservedMemory() {
    return reservedMemory;
  }

  @Override
  public String getRmSchedulerAddr() {
    return rmSchedulerAddr;
  }

  @Override
  public String getLogLevel() {
    return logLevel;
  }

  @Override
  public Map<String, Map<String, LogEntry.Level>> getLogLevelArguments() {
    return logLevelArguments;
  }
}
