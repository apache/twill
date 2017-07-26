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
package org.apache.twill.internal.zookeeper;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executor;

/**
 *
 */
public final class InMemoryZKServer implements Service {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryZKServer.class);

  private final File dataDir;
  private final int tickTime;
  private final boolean autoClean;
  private final int port;
  private final Service delegateService = new AbstractIdleService() {
    @Override
    protected void startUp() throws Exception {
      ZooKeeperServer zkServer = new ZooKeeperServer();
      FileTxnSnapLog ftxn = new FileTxnSnapLog(dataDir, dataDir);
      zkServer.setTxnLogFactory(ftxn);
      zkServer.setTickTime(tickTime);

      factory = ServerCnxnFactory.createFactory();
      factory.configure(getAddress(port), -1);
      factory.startup(zkServer);

      LOG.info("In memory ZK started: " + getConnectionStr());
    }

    @Override
    protected void shutDown() throws Exception {
      try {
        factory.shutdown();
      } finally {
        if (autoClean) {
          cleanDir(dataDir);
        }
      }
    }
  };

  private ServerCnxnFactory factory;

  public static Builder builder() {
    return new Builder();
  }

  private InMemoryZKServer(File dataDir, int tickTime, boolean autoClean, int port) {
    if (dataDir == null) {
      dataDir = Files.createTempDir();
      autoClean = true;
    } else {
      Preconditions.checkArgument(dataDir.isDirectory() || dataDir.mkdirs() || dataDir.isDirectory());
    }

    this.dataDir = dataDir;
    this.tickTime = tickTime;
    this.autoClean = autoClean;
    this.port = port;
  }

  public String getConnectionStr() {
    InetSocketAddress addr = factory.getLocalAddress();
    return String.format("%s:%d", addr.getHostName(), addr.getPort());
  }

  public InetSocketAddress getLocalAddress() {
    return factory.getLocalAddress();
  }

  private InetSocketAddress getAddress(int port) {
    try {
      int socketPort = port < 0 ? 0 : port;
      // This property is needed so that in certain CI environment (e.g. Travis-CI) it can only works properly if
      // it is binded to the wildcard (0.0.0.0) address
      if (Boolean.parseBoolean(System.getProperties().getProperty("twill.zk.server.localhost", "true"))) {
        return new InetSocketAddress(InetAddress.getLocalHost(), socketPort);
      } else {
        return new InetSocketAddress(socketPort);
      }
    } catch (UnknownHostException e) {
      throw Throwables.propagate(e);
    }
  }

  private void cleanDir(File dir) {
    File[] files = dir.listFiles();
    if (files == null) {
      return;
    }
    for (File file : files) {
      if (file.isDirectory()) {
        cleanDir(file);
      }
      file.delete();
    }
  }

  @Override
  public ListenableFuture<State> start() {
    return delegateService.start();
  }

  @Override
  public State startAndWait() {
    return delegateService.startAndWait();
  }

  @Override
  public boolean isRunning() {
    return delegateService.isRunning();
  }

  @Override
  public State state() {
    return delegateService.state();
  }

  @Override
  public ListenableFuture<State> stop() {
    return delegateService.stop();
  }

  @Override
  public State stopAndWait() {
    return delegateService.stopAndWait();
  }

  @Override
  public void addListener(Listener listener, Executor executor) {
    delegateService.addListener(listener, executor);
  }

  /**
   * Builder for creating instance of {@link InMemoryZKServer}.
   */
  public static final class Builder {
    private File dataDir;
    private boolean autoCleanDataDir = false;
    private int tickTime = ZooKeeperServer.DEFAULT_TICK_TIME;
    private int port = -1;

    public Builder setDataDir(File dataDir) {
      this.dataDir = dataDir;
      return this;
    }

    public Builder setAutoCleanDataDir(boolean auto) {
      this.autoCleanDataDir = auto;
      return this;
    }

    public Builder setTickTime(int tickTime) {
      this.tickTime = tickTime;
      return this;
    }

    public Builder setPort(int port) {
      this.port = port;
      return this;
    }

    public InMemoryZKServer build() {
      return new InMemoryZKServer(dataDir, tickTime, autoCleanDataDir, port);
    }

    private Builder() {
    }
  }
}
