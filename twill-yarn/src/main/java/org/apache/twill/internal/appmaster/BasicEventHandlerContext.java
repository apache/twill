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

import org.apache.twill.api.EventHandlerContext;
import org.apache.twill.api.EventHandlerSpecification;
import org.apache.twill.api.RunId;
import org.apache.twill.internal.TwillRuntimeSpecification;

/**
 *
 */
final class BasicEventHandlerContext implements EventHandlerContext {

  private final String applicationName;
  private final RunId runId;
  private final EventHandlerSpecification specification;

  public BasicEventHandlerContext(TwillRuntimeSpecification twillRuntimeSpec) {
    this.applicationName = twillRuntimeSpec.getTwillAppName();
    this.runId = twillRuntimeSpec.getTwillAppRunId();
    this.specification = twillRuntimeSpec.getTwillSpecification().getEventHandler();
  }

  @Override
  public String getApplicationName() {
    return applicationName;
  }

  @Override
  public RunId getRunId() {
    return runId;
  }

  @Override
  public EventHandlerSpecification getSpecification() {
    return specification;
  }
}
