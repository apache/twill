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

import org.apache.twill.api.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public final class LogOnlyEventHandler extends EventHandler {

  private static final Logger LOG = LoggerFactory.getLogger(LogOnlyEventHandler.class);

  @Override
  public TimeoutAction launchTimeout(Iterable<TimeoutEvent> timeoutEvents) {
    for (TimeoutEvent event : timeoutEvents) {
      LOG.info("Requested {} containers for runnable {}, only got {} after {} ms.",
               event.getExpectedInstances(), event.getRunnableName(),
               event.getActualInstances(), System.currentTimeMillis() - event.getRequestTime());
    }
    return TimeoutAction.recheck(Constants.PROVISION_TIMEOUT, TimeUnit.MILLISECONDS);

  }
}
