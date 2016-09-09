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
package org.apache.twill.discovery;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Base class for testing different discovery service implementation.
 */
public abstract class DiscoveryServiceTestBase {

  protected abstract Map.Entry<DiscoveryService, DiscoveryServiceClient> create();

  @Test
  public void simpleDiscoverable() throws Exception {
    final String payload = "data";
    Map.Entry<DiscoveryService, DiscoveryServiceClient> entry = create();
    DiscoveryService discoveryService = entry.getKey();
    DiscoveryServiceClient discoveryServiceClient = entry.getValue();

    // Register one service running on one host:port
    Cancellable cancellable = register(discoveryService, "foo", "localhost", 8090, payload.getBytes());

    // Discover that registered host:port.
    ServiceDiscovered serviceDiscovered = discoveryServiceClient.discover("foo");
    Assert.assertTrue(waitTillExpected(1, serviceDiscovered));

    Discoverable discoverable = new Discoverable("foo", new InetSocketAddress("localhost", 8090), payload.getBytes());

    // Check it exists.
    Assert.assertTrue(serviceDiscovered.contains(discoverable));

    // Remove the service
    cancellable.cancel();

    // There should be no service.
    Assert.assertTrue(waitTillExpected(0, serviceDiscovered));

    Assert.assertFalse(serviceDiscovered.contains(discoverable));
  }

  @Test
  public void testChangeListener() throws InterruptedException {
    Map.Entry<DiscoveryService, DiscoveryServiceClient> entry = create();
    DiscoveryService discoveryService = entry.getKey();
    DiscoveryServiceClient discoveryServiceClient = entry.getValue();

    // Start discovery
    String serviceName = "listener_test";
    ServiceDiscovered serviceDiscovered = discoveryServiceClient.discover(serviceName);

    // Watch for changes.
    final BlockingQueue<List<Discoverable>> events = new ArrayBlockingQueue<List<Discoverable>>(10);
    serviceDiscovered.watchChanges(new ServiceDiscovered.ChangeListener() {
      @Override
      public void onChange(ServiceDiscovered serviceDiscovered) {
        events.add(ImmutableList.copyOf(serviceDiscovered));
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    // An empty list will be received first, as no endpoint has been registered.
    List<Discoverable> discoverables = events.poll(20, TimeUnit.SECONDS);
    Assert.assertNotNull(discoverables);
    Assert.assertTrue(discoverables.isEmpty());

    // Register a service
    Cancellable cancellable = register(discoveryService, serviceName, "localhost", 10000);

    discoverables = events.poll(20, TimeUnit.SECONDS);
    Assert.assertNotNull(discoverables);
    Assert.assertEquals(1, discoverables.size());

    // Register another service endpoint
    Cancellable cancellable2 = register(discoveryService, serviceName, "localhost", 10001);

    discoverables = events.poll(20, TimeUnit.SECONDS);
    Assert.assertNotNull(discoverables);
    Assert.assertEquals(2, discoverables.size());

    // Cancel both of them
    cancellable.cancel();
    cancellable2.cancel();

    // There could be more than one event triggered, but the last event should be an empty list.
    discoverables = events.poll(20, TimeUnit.SECONDS);
    Assert.assertNotNull(discoverables);
    if (!discoverables.isEmpty()) {
      discoverables = events.poll(20, TimeUnit.SECONDS);
    }

    Assert.assertTrue(discoverables.isEmpty());
  }

  @Test
  public void testCancelChangeListener() throws InterruptedException {
    Map.Entry<DiscoveryService, DiscoveryServiceClient> entry = create();
    DiscoveryService discoveryService = entry.getKey();
    DiscoveryServiceClient discoveryServiceClient = entry.getValue();

    String serviceName = "cancel_listener";
    ServiceDiscovered serviceDiscovered = discoveryServiceClient.discover(serviceName);

    // An executor that delay execute a Runnable. It's for testing race because listener cancel and discovery changes.
    Executor delayExecutor = new Executor() {
      @Override
      public void execute(final Runnable command) {
        Thread t = new Thread() {
          @Override
          public void run() {
            try {
              TimeUnit.SECONDS.sleep(2);
              command.run();
            } catch (InterruptedException e) {
              throw Throwables.propagate(e);
            }
          }
        };
        t.start();
      }
    };

    final BlockingQueue<List<Discoverable>> events = new ArrayBlockingQueue<List<Discoverable>>(10);
    Cancellable cancelWatch = serviceDiscovered.watchChanges(new ServiceDiscovered.ChangeListener() {
      @Override
      public void onChange(ServiceDiscovered serviceDiscovered) {
        events.add(ImmutableList.copyOf(serviceDiscovered));
      }
    }, delayExecutor);

    // Wait for the init event call
    Assert.assertNotNull(events.poll(3, TimeUnit.SECONDS));

    // Register a new service endpoint, wait a short while and then cancel the listener
    register(discoveryService, serviceName, "localhost", 1);
    TimeUnit.SECONDS.sleep(1);
    cancelWatch.cancel();

    // The change listener shouldn't get any event, since the invocation is delayed by the executor.
    Assert.assertNull(events.poll(3, TimeUnit.SECONDS));
  }

  @Test
  public void manySameDiscoverable() throws Exception {
    Map.Entry<DiscoveryService, DiscoveryServiceClient> entry = create();
    DiscoveryService discoveryService = entry.getKey();
    DiscoveryServiceClient discoveryServiceClient = entry.getValue();

    List<Cancellable> cancellables = Lists.newArrayList();

    cancellables.add(register(discoveryService, "manyDiscoverable", "localhost", 1));
    cancellables.add(register(discoveryService, "manyDiscoverable", "localhost", 2));
    cancellables.add(register(discoveryService, "manyDiscoverable", "localhost", 3));
    cancellables.add(register(discoveryService, "manyDiscoverable", "localhost", 4));
    cancellables.add(register(discoveryService, "manyDiscoverable", "localhost", 5));

    ServiceDiscovered serviceDiscovered = discoveryServiceClient.discover("manyDiscoverable");
    Assert.assertTrue(waitTillExpected(5, serviceDiscovered));

    for (int i = 0; i < 5; i++) {
      cancellables.get(i).cancel();
      Assert.assertTrue(waitTillExpected(4 - i, serviceDiscovered));
    }
  }

  @Test
  public void multiServiceDiscoverable() throws Exception {
    Map.Entry<DiscoveryService, DiscoveryServiceClient> entry = create();
    DiscoveryService discoveryService = entry.getKey();
    DiscoveryServiceClient discoveryServiceClient = entry.getValue();

    List<Cancellable> cancellables = Lists.newArrayList();

    cancellables.add(register(discoveryService, "service1", "localhost", 1));
    cancellables.add(register(discoveryService, "service1", "localhost", 2));
    cancellables.add(register(discoveryService, "service1", "localhost", 3));
    cancellables.add(register(discoveryService, "service1", "localhost", 4));
    cancellables.add(register(discoveryService, "service1", "localhost", 5));

    cancellables.add(register(discoveryService, "service2", "localhost", 1));
    cancellables.add(register(discoveryService, "service2", "localhost", 2));
    cancellables.add(register(discoveryService, "service2", "localhost", 3));

    cancellables.add(register(discoveryService, "service3", "localhost", 1));
    cancellables.add(register(discoveryService, "service3", "localhost", 2));

    ServiceDiscovered serviceDiscovered = discoveryServiceClient.discover("service1");
    Assert.assertTrue(waitTillExpected(5, serviceDiscovered));

    serviceDiscovered = discoveryServiceClient.discover("service2");
    Assert.assertTrue(waitTillExpected(3, serviceDiscovered));

    serviceDiscovered = discoveryServiceClient.discover("service3");
    Assert.assertTrue(waitTillExpected(2, serviceDiscovered));

    cancellables.add(register(discoveryService, "service3", "localhost", 3));
    Assert.assertTrue(waitTillExpected(3, serviceDiscovered)); // Shows live iterator.

    for (Cancellable cancellable : cancellables) {
      cancellable.cancel();
    }

    Assert.assertTrue(waitTillExpected(0, discoveryServiceClient.discover("service1")));
    Assert.assertTrue(waitTillExpected(0, discoveryServiceClient.discover("service2")));
    Assert.assertTrue(waitTillExpected(0, discoveryServiceClient.discover("service3")));
  }

  @Test
  public void testIterator() throws InterruptedException {
    // This test is to verify TWILL-75
    Map.Entry<DiscoveryService, DiscoveryServiceClient> entry = create();
    final DiscoveryService service = entry.getKey();
    DiscoveryServiceClient client = entry.getValue();

    final String serviceName = "iterator";
    ServiceDiscovered discovered = client.discover(serviceName);

    // Create a thread for performing registration.
    Thread t = new Thread() {
      @Override
      public void run() {
        service.register(new Discoverable(serviceName, new InetSocketAddress(12345), new byte[]{}));
      }
    };

    Iterator<Discoverable> iterator = discovered.iterator();
    t.start();
    t.join();

    // This would throw exception if there is race condition.
    Assert.assertFalse(iterator.hasNext());
  }

  protected Cancellable register(DiscoveryService service, final String name, final String host, final int port) {
    return register(service, name, host, port, new byte[]{});
  }

  protected Cancellable register(DiscoveryService service, final String name, final String host, final int port,
                                 final byte[] payload) {
    return service.register(new Discoverable(name, new InetSocketAddress(host, port), payload));
  }

  protected boolean waitTillExpected(final int expected, ServiceDiscovered serviceDiscovered) {
    final CountDownLatch latch = new CountDownLatch(1);
    serviceDiscovered.watchChanges(new ServiceDiscovered.ChangeListener() {
      @Override
      public void onChange(ServiceDiscovered serviceDiscovered) {
        if (expected == Iterables.size(serviceDiscovered)) {
          latch.countDown();
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    try {
      return latch.await(60, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }
}
