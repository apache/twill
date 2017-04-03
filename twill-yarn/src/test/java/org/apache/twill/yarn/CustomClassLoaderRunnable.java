/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.yarn;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.ServiceAnnouncer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * Runnable for testing custom classloader
 */
public final class CustomClassLoaderRunnable extends AbstractTwillRunnable {

  static final String SERVICE_NAME = "custom.service";
  static final String GENERATED_CLASS_NAME = "org.apache.twill.test.Generated";

  private static final Logger LOG = LoggerFactory.getLogger(CustomClassLoaderRunnable.class);

  private final CountDownLatch stopLatch = new CountDownLatch(1);

  @Override
  public void run() {
    try {
      Class<?> cls = Class.forName(GENERATED_CLASS_NAME);
      java.lang.reflect.Method announce = cls.getMethod("announce", ServiceAnnouncer.class, String.class, int.class);
      announce.invoke(cls.newInstance(), getContext(), SERVICE_NAME, 54321);
      Uninterruptibles.awaitUninterruptibly(stopLatch);
    } catch (Exception e) {
      LOG.error("Failed to call announce on " + GENERATED_CLASS_NAME, e);
    }
  }

  @Override
  public void stop() {
    stopLatch.countDown();
  }
}
