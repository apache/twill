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
package org.apache.twill.api;

import org.apache.twill.internal.Constants;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A callback handler for acting on application events related to {@link TwillRunnable} lifecycle events.
 */
public abstract class EventHandler {

  protected EventHandlerContext context;

  /**
   * Represents action to act upon runnable launch timeout.
   */
  public static final class TimeoutAction {

    // Next timeout in milliseconds.
    private final long timeout;

    /**
     * Creates a {@link TimeoutAction} to indicate aborting the application.
     */
    public static TimeoutAction abort() {
      return new TimeoutAction(-1);
    }

    /**
     * Creates a {@link TimeoutAction} to indicate recheck again after the given time has passed.
     * @param elapse Time to elapse before checking for the timeout again.
     * @param unit Unit of the elapse time.
     */
    public static TimeoutAction recheck(long elapse, TimeUnit unit) {
      return new TimeoutAction(TimeUnit.MILLISECONDS.convert(elapse, unit));
    }

    private TimeoutAction(long timeout) {
      this.timeout = timeout;
    }

    /**
     * Returns timeout in milliseconds or {@code -1} if to abort the application.
     */
    public long getTimeout() {
      return timeout;
    }
  }

  /**
   * This class holds information about a launch timeout event.
   */
  public static final class TimeoutEvent {
    private final String runnableName;
    private final int expectedInstances;
    private final int actualInstances;
    private final long requestTime;

    public TimeoutEvent(String runnableName, int expectedInstances, int actualInstances, long requestTime) {
      this.runnableName = runnableName;
      this.expectedInstances = expectedInstances;
      this.actualInstances = actualInstances;
      this.requestTime = requestTime;
    }

    public String getRunnableName() {
      return runnableName;
    }

    public int getExpectedInstances() {
      return expectedInstances;
    }

    public int getActualInstances() {
      return actualInstances;
    }

    public long getRequestTime() {
      return requestTime;
    }
  }

  /**
   * Returns an {@link EventHandlerSpecification} for configuring this handler class.
   */
  public EventHandlerSpecification configure() {
    return new EventHandlerSpecification() {
      @Override
      public String getClassName() {
        return EventHandler.this.getClass().getName();
      }

      @Override
      public Map<String, String> getConfigs() {
        return EventHandler.this.getConfigs();
      }
    };
  }

  /**
   * Invoked by the application to initialize this EventHandler instance.
   *
   * @param context context object for accessing the event handler execution context.
   */
  public void initialize(EventHandlerContext context) {
    this.context = context;
  }

  /**
   * Invoked by the application when it starts.
   */
  public void started() {
    // No-op
  }

  /**
   * Invoked by the application when new container is launched for a {@link TwillRunnable}.
   *
   * @param runnableName name of the runnable to be run in the new container
   * @param instanceId the instance ID of the runnable instance to be run in the new container
   * @param containerId the ID of the newly launched container
   */
  public void containerLaunched(String runnableName, int instanceId, String containerId) {
    // No-op
  }

  /**
   * Invoked by the application when the container allocated for a {@link TwillRunnable} is stopped.
   *
   * @param runnableName name of the runnable in the stopped container
   * @param instanceId the instance ID of the runnable instance run in the stopped container
   * @param containerId the ID of the stopped container
   * @param exitStatus the exit status of the container
   */
  public void containerStopped(String runnableName, int instanceId, String containerId, int exitStatus) {
    // No-op
  }

  /**
   * Invoked by the application when all containers complete.
   */
  public void completed() {
    // No-op
  }

  /**
   * Invoked by the application when stop command is received to kill the current application.
   *
   */
  public void killed() {
    // No-op
  }

  /**
   * Invoked by the application when the application is aborted because of timeout.
   */
  public void aborted() {
    // No-op
  }

  /**
   * Invoked by the application when shutting down.
   */
  public void destroy() {
    // No-op
  }

  /**
   * Invoked when the number of expected instances doesn't match with number of actual instances.
   * @param timeoutEvents An Iterable of {@link TimeoutEvent} that contains information about runnable launch timeout.
   * @return A {@link TimeoutAction} to govern action to act. By default it is to recheck after 60 seconds.
   */
  public TimeoutAction launchTimeout(Iterable<TimeoutEvent> timeoutEvents) {
    return new TimeoutAction(Constants.PROVISION_TIMEOUT);
  }

  /**
   * Returns set of configurations available at runtime for access.
   */
  protected Map<String, String> getConfigs() {
    return Collections.emptyMap();
  }
}
