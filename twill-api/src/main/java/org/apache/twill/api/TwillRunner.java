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

import org.apache.twill.api.security.SecureStoreRenewer;
import org.apache.twill.api.security.SecureStoreWriter;
import org.apache.twill.common.Cancellable;

import java.util.concurrent.TimeUnit;

/**
 * This interface prepares execution of {@link TwillRunnable} and {@link TwillApplication}.
 */
public interface TwillRunner {

  /**
   * Interface to represents information of a live application.
   */
  interface LiveInfo {

    /**
     * Returns name of the application.
     * @return Application name as a {@link String}.
     */
    String getApplicationName();

    /**
     * Returns {@link TwillController}s for all live instances of the application.
     * @return An {@link Iterable} of {@link TwillController}.
     */
    Iterable<TwillController> getControllers();
  }

  /**
   * Prepares to run the given {@link TwillRunnable} with {@link ResourceSpecification#BASIC} resource specification.
   * The name for the runnable will be defaulted to {@code runnable.getClass().getSimpleName()}
   * 
   * @param runnable The runnable to run through Twill when {@link TwillPreparer#start()} is called.
   * @return A {@link TwillPreparer} for setting up runtime options.
   */
  TwillPreparer prepare(TwillRunnable runnable);

  /**
   * Prepares to run the given {@link TwillRunnable} with the given resource specification. The name for the runnable
   * will be defaulted to {@code runnable.getClass().getSimpleName()}
   * 
   * @param runnable The runnable to run through Twill when {@link TwillPreparer#start()} is called.
   * @param resourceSpecification The resource specification for running the runnable.
   * @return A {@link TwillPreparer} for setting up runtime options.
   */
  TwillPreparer prepare(TwillRunnable runnable, ResourceSpecification resourceSpecification);

  /**
   * Prepares to run the given {@link TwillApplication} as specified by the application.
   * @param application The application to run through Twill when {@link TwillPreparer#start()} is called.
   * @return A {@link TwillPreparer} for setting up runtime options.
   */
  TwillPreparer prepare(TwillApplication application);

  /**
   * Gets a {@link TwillController} for the given application and runId.
   * @param applicationName Name of the application.
   * @param runId The runId of the running application.
   * @return A {@link TwillController} to interact with the application or null if no such runId is found.
   */
  TwillController lookup(String applicationName, RunId runId);

  /**
   * Gets an {@link Iterable} of {@link TwillController} for all running instances of the given application.
   * @param applicationName Name of the application.
   * @return A live {@link Iterable} that gives the latest {@link TwillController} set for all running
   *         instances of the application when {@link Iterable#iterator()} is invoked.
   */
  Iterable<TwillController> lookup(String applicationName);

  /**
   * Gets an {@link Iterable} of {@link LiveInfo}.
   * @return A live {@link Iterable} that gives the latest information on the set of applications that
   *         have running instances when {@link Iterable#iterator()}} is invoked.
   */
  Iterable<LiveInfo> lookupLive();

  /**
   * Schedules a periodic update of SecureStore. The first call to the given {@link SecureStoreUpdater} will be made
   * after {@code initialDelay}, and subsequently with the given {@code delay} between completion of one update
   * and starting of the next. If exception is thrown on call
   * {@link SecureStoreUpdater#update(String, RunId)}, the exception will only get logged
   * and won't suppress the next update call.
   *
   * @param updater A {@link SecureStoreUpdater} for creating new SecureStore.
   * @param initialDelay Delay before the first call to update method.
   * @param delay Delay between completion of one update call to the next one.
   * @param unit time unit for the initialDelay and delay.
   * @return A {@link Cancellable} for cancelling the scheduled update.
   *
   * @deprecated Use {@link #setSecureStoreRenewer(SecureStoreRenewer, long, long, long, TimeUnit)} instead.
   */
  @Deprecated
  Cancellable scheduleSecureStoreUpdate(final SecureStoreUpdater updater,
                                        long initialDelay, long delay, TimeUnit unit);

  /**
   * Sets and schedules a periodic renewal of {@link SecureStore} using a given {@link SecureStoreRenewer}.
   * There is always only one active {@link SecureStoreRenewer}. Setting a new renewer will replace the old one
   * and setting up a new schedule.
   *
   * @param renewer a {@link SecureStoreRenewer} for renewing {@link SecureStore} for all applications.
   * @param initialDelay delay before the first call to renew method.
   * @param delay the delay between successful completion of one renew call to the next one.
   * @param retryDelay the delay before the retrying the renewal if the call
   *                   to {@link SecureStoreRenewer#renew(String, RunId, SecureStoreWriter)} raised exception.
   * @param unit time unit for the initialDelay and period.
   */
  Cancellable setSecureStoreRenewer(SecureStoreRenewer renewer,
                                    long initialDelay, long delay, long retryDelay, TimeUnit unit);
}
