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
package org.apache.twill.zookeeper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.internal.zookeeper.KillZKSession;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class ZKClientTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testChroot() throws Exception {
    InMemoryZKServer zkServer = InMemoryZKServer.builder().setTickTime(1000).build();
    zkServer.startAndWait();

    try {
      ZKClientService client = ZKClientService.Builder.of(zkServer.getConnectionStr() + "/chroot").build();
      client.startAndWait();
      try {
        List<OperationFuture<String>> futures = Lists.newArrayList();
        futures.add(client.create("/test1/test2", null, CreateMode.PERSISTENT));
        futures.add(client.create("/test1/test3", null, CreateMode.PERSISTENT));
        Futures.successfulAsList(futures).get();

        Assert.assertNotNull(client.exists("/test1/test2").get());
        Assert.assertNotNull(client.exists("/test1/test3").get());

      } finally {
        client.stopAndWait();
      }
    } finally {
      zkServer.stopAndWait();
    }
  }

  @Test
  public void testCreateParent() throws ExecutionException, InterruptedException {
    InMemoryZKServer zkServer = InMemoryZKServer.builder().setTickTime(1000).build();
    zkServer.startAndWait();

    try {
      ZKClientService client = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
      client.startAndWait();

      try {
        String path = client.create("/test1/test2/test3/test4/test5",
                                    "testing".getBytes(), CreateMode.PERSISTENT_SEQUENTIAL).get();
        Assert.assertTrue(path.startsWith("/test1/test2/test3/test4/test5"));

        String dataPath = "";
        for (int i = 1; i <= 4; i++) {
          dataPath = dataPath + "/test" + i;
          Assert.assertNull(client.getData(dataPath).get().getData());
        }
        Assert.assertTrue(Arrays.equals("testing".getBytes(), client.getData(path).get().getData()));
      } finally {
        client.stopAndWait();
      }
    } finally {
      zkServer.stopAndWait();
    }
  }

  @Test
  public void testGetChildren() throws ExecutionException, InterruptedException {
    InMemoryZKServer zkServer = InMemoryZKServer.builder().setTickTime(1000).build();
    zkServer.startAndWait();

    try {
      ZKClientService client = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
      client.startAndWait();

      try {
        client.create("/test", null, CreateMode.PERSISTENT).get();
        Assert.assertTrue(client.getChildren("/test").get().getChildren().isEmpty());

        Futures.allAsList(ImmutableList.of(client.create("/test/c1", null, CreateMode.EPHEMERAL),
                                           client.create("/test/c2", null, CreateMode.EPHEMERAL))).get();

        NodeChildren nodeChildren = client.getChildren("/test").get();
        Assert.assertEquals(2, nodeChildren.getChildren().size());

        Assert.assertEquals(ImmutableSet.of("c1", "c2"), ImmutableSet.copyOf(nodeChildren.getChildren()));

      } finally {
        client.stopAndWait();
      }
    } finally {
      zkServer.stopAndWait();
    }
  }

  @Test
  public void testSetData() throws ExecutionException, InterruptedException {
    InMemoryZKServer zkServer = InMemoryZKServer.builder().setTickTime(1000).build();
    zkServer.startAndWait();

    try {
      ZKClientService client = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
      client.startAndWait();

      client.create("/test", null, CreateMode.PERSISTENT).get();
      Assert.assertNull(client.getData("/test").get().getData());

      client.setData("/test", "testing".getBytes()).get();
      Assert.assertTrue(Arrays.equals("testing".getBytes(), client.getData("/test").get().getData()));

    } finally {
      zkServer.stopAndWait();
    }
  }

  @Test
  public void testExpireRewatch() throws InterruptedException, IOException, ExecutionException {
    InMemoryZKServer zkServer = InMemoryZKServer.builder().setTickTime(1000).build();
    zkServer.startAndWait();

    try {
      final CountDownLatch expireReconnectLatch = new CountDownLatch(1);
      final AtomicBoolean expired = new AtomicBoolean(false);
      final ZKClientService client = ZKClientServices.delegate(ZKClients.reWatchOnExpire(
                                        ZKClientService.Builder.of(zkServer.getConnectionStr())
                                                       .setSessionTimeout(2000)
                                                       .setConnectionWatcher(new Watcher() {
            @Override
            public void process(WatchedEvent event) {
              if (event.getState() == Event.KeeperState.Expired) {
                expired.set(true);
              } else if (event.getState() == Event.KeeperState.SyncConnected && expired.compareAndSet(true, true)) {
                expireReconnectLatch.countDown();
              }
            }
          }).build()));
      client.startAndWait();

      try {
        final BlockingQueue<Watcher.Event.EventType> events = new LinkedBlockingQueue<Watcher.Event.EventType>();
        client.exists("/expireRewatch", new Watcher() {
          @Override
          public void process(WatchedEvent event) {
            client.exists("/expireRewatch", this);
            events.add(event.getType());
          }
        });

        client.create("/expireRewatch", null, CreateMode.PERSISTENT);
        Assert.assertEquals(Watcher.Event.EventType.NodeCreated, events.poll(2, TimeUnit.SECONDS));

        KillZKSession.kill(client.getZooKeeperSupplier().get(), zkServer.getConnectionStr(), 1000);

        Assert.assertTrue(expireReconnectLatch.await(5, TimeUnit.SECONDS));

        client.delete("/expireRewatch");
        Assert.assertEquals(Watcher.Event.EventType.NodeDeleted, events.poll(4, TimeUnit.SECONDS));
      } finally {
        client.stopAndWait();
      }
    } finally {
      zkServer.stopAndWait();
    }
  }

  @Test
  public void testRetry() throws ExecutionException, InterruptedException, TimeoutException, IOException {
    File dataDir = tmpFolder.newFolder();
    InMemoryZKServer zkServer = InMemoryZKServer.builder().setDataDir(dataDir).setTickTime(1000).build();
    zkServer.startAndWait();
    int port = zkServer.getLocalAddress().getPort();

    final CountDownLatch disconnectLatch = new CountDownLatch(1);
    ZKClientService client = ZKClientServices.delegate(ZKClients.retryOnFailure(
      ZKClientService.Builder.of(zkServer.getConnectionStr()).setConnectionWatcher(new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.Disconnected) {
          disconnectLatch.countDown();
        }
      }
    }).build(), RetryStrategies.fixDelay(0, TimeUnit.SECONDS)));
    client.startAndWait();

    zkServer.stopAndWait();

    Assert.assertTrue(disconnectLatch.await(1, TimeUnit.SECONDS));

    final CountDownLatch createLatch = new CountDownLatch(1);
    Futures.addCallback(client.create("/testretry/test", null, CreateMode.PERSISTENT), new FutureCallback<String>() {
      @Override
      public void onSuccess(String result) {
        createLatch.countDown();
      }

      @Override
      public void onFailure(Throwable t) {
        t.printStackTrace(System.out);
      }
    });

    TimeUnit.SECONDS.sleep(2);
    zkServer = InMemoryZKServer.builder()
                               .setDataDir(dataDir)
                               .setAutoCleanDataDir(true)
                               .setPort(port)
                               .setTickTime(1000)
                               .build();
    zkServer.startAndWait();

    try {
      Assert.assertTrue(createLatch.await(5, TimeUnit.SECONDS));
    } finally {
      zkServer.stopAndWait();
    }
  }

  @Test
  public void testACL() throws IOException, ExecutionException, InterruptedException, NoSuchAlgorithmException {
    InMemoryZKServer zkServer = InMemoryZKServer.builder().setDataDir(tmpFolder.newFolder()).setTickTime(1000).build();
    zkServer.startAndWait();

    try {
      String userPass = "user:pass";
      String digest = DigestAuthenticationProvider.generateDigest(userPass);

      // Creates two zkclients
      ZKClientService zkClient = ZKClientService.Builder
                                                .of(zkServer.getConnectionStr())
                                                .addAuthInfo("digest", userPass.getBytes())
                                                .build();
      zkClient.startAndWait();

      ZKClientService noAuthClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
      noAuthClient.startAndWait();


      // Create a node that is readable by all client, but admin for the creator
      String path = "/testacl";
      zkClient.create(path, "test".getBytes(), CreateMode.PERSISTENT,
                      ImmutableList.of(
                        new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE),
                        new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.AUTH_IDS)
                      )).get();

      // Verify the ACL
      ACLData aclData = zkClient.getACL(path).get();
      Assert.assertEquals(2, aclData.getACL().size());
      ACL acl = aclData.getACL().get(1);
      Assert.assertEquals(ZooDefs.Perms.ALL, acl.getPerms());
      Assert.assertEquals("digest", acl.getId().getScheme());
      Assert.assertEquals(digest, acl.getId().getId());

      Assert.assertEquals("test", new String(noAuthClient.getData(path).get().getData()));

      // When tries to write using the no-auth zk client, it should fail.
      try {
        noAuthClient.setData(path, "test2".getBytes()).get();
        Assert.fail();
      } catch (ExecutionException e) {
        Assert.assertTrue(e.getCause() instanceof KeeperException.NoAuthException);
      }

      // Change ACL to make it open for all
      zkClient.setACL(path, ImmutableList.of(new ACL(ZooDefs.Perms.WRITE, ZooDefs.Ids.ANYONE_ID_UNSAFE))).get();

      // Write again with the non-auth client, now should succeed.
      noAuthClient.setData(path, "test2".getBytes()).get();

      noAuthClient.stopAndWait();
      zkClient.stopAndWait();

    } finally {
      zkServer.stopAndWait();
    }
  }
}
