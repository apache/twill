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
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

/**
 * This interface is for adapting differences in ApplicationReport in different Hadoop version.
 */
public interface YarnApplicationReport {

  /**
   * Get the <code>ApplicationId</code> of the application.
   * @return <code>ApplicationId</code> of the application
   */
  ApplicationId getApplicationId();

  /**
   * Get the <code>ApplicationAttemptId</code> of the current
   * attempt of the application.
   * @return <code>ApplicationAttemptId</code> of the attempt
   */
  ApplicationAttemptId getCurrentApplicationAttemptId();

  /**
   * Get the <em>queue</em> to which the application was submitted.
   * @return <em>queue</em> to which the application was submitted
   */
  String getQueue();

  /**
   * Get the user-defined <em>name</em> of the application.
   * @return <em>name</em> of the application
   */
  String getName();

  /**
   * Get the <em>host</em> on which the <code>ApplicationMaster</code>
   * is running.
   * @return <em>host</em> on which the <code>ApplicationMaster</code>
   *         is running
   */
  String getHost();

  /**
   * Get the <em>RPC port</em> of the <code>ApplicationMaster</code>.
   * @return <em>RPC port</em> of the <code>ApplicationMaster</code>
   */
  int getRpcPort();


  /**
   * Get the <code>YarnApplicationState</code> of the application.
   * @return <code>YarnApplicationState</code> of the application
   */
  YarnApplicationState getYarnApplicationState();


  /**
   * Get  the <em>diagnositic information</em> of the application in case of
   * errors.
   * @return <em>diagnositic information</em> of the application in case
   *         of errors
   */
  String getDiagnostics();


  /**
   * Get the <em>tracking url</em> for the application.
   * @return <em>tracking url</em> for the application
   */
  String getTrackingUrl();


  /**
   * Get the original not-proxied <em>tracking url</em> for the application.
   * This is intended to only be used by the proxy itself.
   * @return the original not-proxied <em>tracking url</em> for the application
   */
  String getOriginalTrackingUrl();

  /**
   * Get the <em>start time</em> of the application.
   * @return <em>start time</em> of the application
   */
  long getStartTime();


  /**
   * Get the <em>finish time</em> of the application.
   * @return <em>finish time</em> of the application
   */
  long getFinishTime();


  /**
   * Get the <em>final finish status</em> of the application.
   * @return <em>final finish status</em> of the application
   */
  FinalApplicationStatus getFinalApplicationStatus();

  /**
   * Retrieve the structure containing the job resources for this application.
   * @return the job resources structure for this application
   */
  ApplicationResourceUsageReport getApplicationResourceUsageReport();
}
