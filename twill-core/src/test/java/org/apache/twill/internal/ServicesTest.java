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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Unit test for {@link Services} methods.
 */
public class ServicesTest {

  private static final Logger LOG = LoggerFactory.getLogger(ServicesTest.class);

  @Test
  public void testChain() throws ExecutionException, InterruptedException {
    AtomicBoolean transiting = new AtomicBoolean(false);
    Service s1 = new DummyService("s1", transiting);
    Service s2 = new DummyService("s2", transiting);
    Service s3 = new DummyService("s3", transiting);

    Futures.allAsList(Services.chainStart(s1, s2, s3).get()).get();
    Futures.allAsList(Services.chainStop(s3, s2, s1).get()).get();
  }

  @Test
  public void testCompletion() throws ExecutionException, InterruptedException {
    Service service = new DummyService("s1", new AtomicBoolean());
    ListenableFuture<Service.State> completion = Services.getCompletionFuture(service);

    service.start();
    service.stop();

    completion.get();

    AtomicBoolean transiting = new AtomicBoolean();
    service = new DummyService("s2", transiting);
    completion = Services.getCompletionFuture(service);

    service.startAndWait();
    transiting.set(true);
    service.stop();

    try {
      completion.get();
      Assert.assertTrue(false);
    } catch (ExecutionException e) {
      // Expected
    }
  }

  private static final class DummyService extends AbstractIdleService {

    private final String name;
    private final AtomicBoolean transiting;

    private DummyService(String name, AtomicBoolean transiting) {
      this.name = name;
      this.transiting = transiting;
    }

    @Override
    protected void startUp() throws Exception {
      Preconditions.checkState(transiting.compareAndSet(false, true));
      LOG.info("Starting: " + name);
      TimeUnit.MILLISECONDS.sleep(500);
      LOG.info("Started: " + name);
      Preconditions.checkState(transiting.compareAndSet(true, false));
    }

    @Override
    protected void shutDown() throws Exception {
      Preconditions.checkState(transiting.compareAndSet(false, true));
      LOG.info("Stopping: " + name);
      TimeUnit.MILLISECONDS.sleep(500);
      LOG.info("Stopped: " + name);
      Preconditions.checkState(transiting.compareAndSet(true, false));
    }
  }
}
