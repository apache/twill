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

import com.google.common.collect.Maps;
import org.apache.twill.common.Cancellable;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.internal.zookeeper.KillZKSession;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test Zookeeper based discovery service.
 */
public class ZKDiscoveryServiceTest extends DiscoveryServiceTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ZKDiscoveryServiceTest.class);

  private static InMemoryZKServer zkServer;
  private static ZKClientService zkClient;

  @BeforeClass
  public static void beforeClass() {
    zkServer = InMemoryZKServer.builder().setTickTime(100000).build();
    zkServer.startAndWait();

    zkClient = ZKClientServices.delegate(
      ZKClients.retryOnFailure(
        ZKClients.reWatchOnExpire(
          ZKClientService.Builder.of(zkServer.getConnectionStr()).build()),
        RetryStrategies.fixDelay(1, TimeUnit.SECONDS)));
    zkClient.startAndWait();
  }

  @AfterClass
  public static void afterClass() {
    zkClient.stopAndWait();
    zkServer.stopAndWait();
  }

  @Test (timeout = 30000)
  public void testDoubleRegister() throws Exception {
    Map.Entry<DiscoveryService, DiscoveryServiceClient> entry = create();
    try {
      DiscoveryService discoveryService = entry.getKey();
      DiscoveryServiceClient discoveryServiceClient = entry.getValue();

      // Register on the same host port, it shouldn't fail.
      Cancellable cancellable = register(discoveryService, "test_double_reg", "localhost", 54321);
      Cancellable cancellable2 = register(discoveryService, "test_double_reg", "localhost", 54321);

      ServiceDiscovered discoverables = discoveryServiceClient.discover("test_double_reg");

      Assert.assertTrue(waitTillExpected(1, discoverables));

      cancellable.cancel();
      cancellable2.cancel();

      // Register again with two different clients, but killing session of the first one.
      final ZKClientService zkClient2 = ZKClientServices.delegate(
        ZKClients.retryOnFailure(
          ZKClients.reWatchOnExpire(
            ZKClientService.Builder.of(zkServer.getConnectionStr()).build()),
          RetryStrategies.fixDelay(1, TimeUnit.SECONDS)));
      zkClient2.startAndWait();

      try (ZKDiscoveryService discoveryService2 = new ZKDiscoveryService(zkClient2)) {
        cancellable2 = register(discoveryService2, "test_multi_client", "localhost", 54321);

        // Schedule a thread to shutdown zkClient2.
        new Thread() {
          @Override
          public void run() {
            try {
              TimeUnit.SECONDS.sleep(2);
              zkClient2.stopAndWait();
            } catch (InterruptedException e) {
              LOG.error(e.getMessage(), e);
            }
          }
        }.start();

        // This call would block until zkClient2 is shutdown.
        cancellable = register(discoveryService, "test_multi_client", "localhost", 54321);
        cancellable.cancel();
      } finally {
        zkClient2.stopAndWait();
      }
    } finally {
      closeServices(entry);
    }
  }

  @Test
  public void testSessionExpires() throws Exception {
    Map.Entry<DiscoveryService, DiscoveryServiceClient> entry = create();
    try {
      DiscoveryService discoveryService = entry.getKey();
      DiscoveryServiceClient discoveryServiceClient = entry.getValue();

      Cancellable cancellable = register(discoveryService, "test_expires", "localhost", 54321);

      ServiceDiscovered discoverables = discoveryServiceClient.discover("test_expires");

      // Discover that registered host:port.
      Assert.assertTrue(waitTillExpected(1, discoverables));

      KillZKSession.kill(zkClient.getZooKeeperSupplier().get(), zkServer.getConnectionStr(), 10000);

      // Register one more endpoint to make sure state has been reflected after reconnection
      Cancellable cancellable2 = register(discoveryService, "test_expires", "localhost", 54322);

      // Reconnection would trigger re-registration.
      Assert.assertTrue(waitTillExpected(2, discoverables));

      cancellable.cancel();
      cancellable2.cancel();

      // Verify that both are now gone.
      Assert.assertTrue(waitTillExpected(0, discoverables));
    } finally {
      closeServices(entry);
    }
  }

  @Override
  protected Map.Entry<DiscoveryService, DiscoveryServiceClient> create() {
    DiscoveryService discoveryService = new ZKDiscoveryService(zkClient);
    DiscoveryServiceClient discoveryServiceClient = new ZKDiscoveryService(zkClient);

    return Maps.immutableEntry(discoveryService, discoveryServiceClient);
  }
}
