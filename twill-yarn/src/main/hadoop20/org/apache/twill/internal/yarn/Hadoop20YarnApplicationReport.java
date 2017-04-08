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
package org.apache.twill.internal.yarn;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

/**
 *
 */
public final class Hadoop20YarnApplicationReport implements YarnApplicationReport {

  private final ApplicationReport report;

  public Hadoop20YarnApplicationReport(ApplicationReport report) {
    this.report = report;
  }

  @Override
  public ApplicationId getApplicationId() {
    return report.getApplicationId();
  }

  @Override
  public ApplicationAttemptId getCurrentApplicationAttemptId() {
    return report.getCurrentApplicationAttemptId();
  }

  @Override
  public String getQueue() {
    return report.getQueue();
  }

  @Override
  public String getName() {
    return report.getName();
  }

  @Override
  public String getHost() {
    return report.getHost();
  }

  @Override
  public int getRpcPort() {
    return report.getRpcPort();
  }

  @Override
  public YarnApplicationState getYarnApplicationState() {
    return report.getYarnApplicationState();
  }

  @Override
  public String getDiagnostics() {
    return report.getDiagnostics();
  }

  @Override
  public String getTrackingUrl() {
    return "http://" + report.getTrackingUrl();
  }

  @Override
  public String getOriginalTrackingUrl() {
    return "http://" + report.getOriginalTrackingUrl();
  }

  @Override
  public long getStartTime() {
    return report.getStartTime();
  }

  @Override
  public long getFinishTime() {
    return report.getFinishTime();
  }

  @Override
  public FinalApplicationStatus getFinalApplicationStatus() {
    return report.getFinalApplicationStatus();
  }

  @Override
  public ApplicationResourceUsageReport getApplicationResourceUsageReport() {
    return report.getApplicationResourceUsageReport();
  }
}
