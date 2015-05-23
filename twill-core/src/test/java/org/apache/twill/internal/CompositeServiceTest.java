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
package org.apache.twill.internal;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tests for {@link CompositeService}.
 */
public class CompositeServiceTest {

  @Test
  public void testOrder() throws InterruptedException, ExecutionException, TimeoutException {
    List<Service> services = Lists.newArrayList();

    // Start 10 services and check their start sequence is ordered.
    Semaphore semaphore = new Semaphore(0);
    for (int i = 0; i < 10; i++) {
      services.add(new TestService(semaphore, i));
    }

    Service service = new CompositeService(services);
    service.start().get(5, TimeUnit.SECONDS);

    // There should be 10 permits after all 10 services started
    Assert.assertTrue(semaphore.tryAcquire(10, 5, TimeUnit.SECONDS));

    // Check all services are running
    Assert.assertTrue(Iterables.all(services, serviceStatePredicate(Service.State.RUNNING)));

    // Release 10 permits for the stop sequence to start
    semaphore.release(10);
    service.stop().get(5, TimeUnit.SECONDS);

    // There should be no permit left after all 10 services stopped
    Assert.assertFalse(semaphore.tryAcquire(10));

    // Check all services are stopped
    Assert.assertTrue(Iterables.all(services, serviceStatePredicate(Service.State.TERMINATED)));
  }

  @Test
  public void testErrorStart() throws InterruptedException {
    List<Service> services = Lists.newArrayList();

    // Create 5 services. The forth one will got a start failure.
    Semaphore semaphore = new Semaphore(0);
    for (int i = 0; i < 5; i++) {
      services.add(new TestService(semaphore, i, i == 3));
    }

    Service service = new CompositeService(services);
    try {
      service.start().get();
      Assert.fail();
    } catch (ExecutionException e) {
      // Expected
    }

    // Verify all services are not in running state
    Assert.assertTrue(Iterables.all(services, Predicates.not(serviceStatePredicate(Service.State.RUNNING))));

    // There should be one service in error state
    Assert.assertTrue(Iterables.removeIf(services, serviceStatePredicate(Service.State.FAILED)));

    for (Service s : services) {
      Assert.assertNotEquals(3, ((TestService) s).getOrder());
    }
  }

  private Predicate<Service> serviceStatePredicate(final Service.State state) {
    return new Predicate<Service>() {
      @Override
      public boolean apply(Service service) {
        return service.state() == state;
      }
    };
  }

  private static final class TestService extends AbstractIdleService {

    private final Semaphore semaphore;
    private final int order;
    private final boolean startFail;

    private TestService(Semaphore semaphore, int order) {
      this(semaphore, order, false);
    }

    private TestService(Semaphore semaphore, int order, boolean startFail) {
      this.semaphore = semaphore;
      this.order = order;
      this.startFail = startFail;
    }

    public int getOrder() {
      return order;
    }

    @Override
    protected void startUp() throws Exception {
      Preconditions.checkState(!startFail, "Fail to start service of order %s", order);

      // Acquire all permits emitted by previous service
      semaphore.acquire(order);
      // Emit enough permits for next service
      semaphore.release(order + 1);
    }

    @Override
    protected void shutDown() throws Exception {
      semaphore.acquire(order + 1);
      semaphore.release(order);
    }
  }
}
