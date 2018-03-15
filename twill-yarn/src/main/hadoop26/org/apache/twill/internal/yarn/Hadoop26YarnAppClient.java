/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.twill.internal.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.twill.api.Configs;

/**
 * <p>
 * The service implementation of {@link YarnAppClient} for Apache Hadoop 2.6 and beyond.
 *
 * The {@link VersionDetectYarnAppClientFactory} class will decide to return instance of this class for
 * Apache Hadoop 2.6 and beyond.
 * </p>
 */
@SuppressWarnings("unused")
public class Hadoop26YarnAppClient extends Hadoop23YarnAppClient {

  public Hadoop26YarnAppClient(Configuration configuration) {
    super(configuration);
  }

  @Override
  protected void configureAppSubmissionContext(ApplicationSubmissionContext context) {
    super.configureAppSubmissionContext(context);
    long interval = configuration.getLong(Configs.Keys.YARN_ATTEMPT_FAILURES_VALIDITY_INTERVAL, -1L);
    if (interval > 0) {
      context.setAttemptFailuresValidityInterval(interval);
    }
  }
}
