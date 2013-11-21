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
package org.apache.twill.internal.kafka.client;

import com.google.common.collect.Maps;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.ChannelGroupFutureListener;
import org.jboss.netty.channel.group.DefaultChannelGroup;

import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

/**
 * Provides netty socket connection reuse.
 */
final class ConnectionPool {

  private final ClientBootstrap bootstrap;
  private final ChannelGroup channelGroup;
  private final ConcurrentMap<InetSocketAddress, Queue<ChannelFuture>> connections;

  /**
   * For releasing a connection back to the pool.
   */
  interface ConnectionReleaser {
    void release();
  }

  /**
   * Result of a connect request.
   */
  interface ConnectResult extends ConnectionReleaser {
    ChannelFuture getChannelFuture();
  }

  ConnectionPool(ClientBootstrap bootstrap) {
    this.bootstrap = bootstrap;
    this.channelGroup = new DefaultChannelGroup();
    this.connections = Maps.newConcurrentMap();
  }

  ConnectResult connect(InetSocketAddress address) {
    Queue<ChannelFuture> channelFutures = connections.get(address);
    if (channelFutures == null) {
      channelFutures = new ConcurrentLinkedQueue<ChannelFuture>();
      Queue<ChannelFuture> result = connections.putIfAbsent(address, channelFutures);
      channelFutures = result == null ? channelFutures : result;
    }

    ChannelFuture channelFuture = channelFutures.poll();
    while (channelFuture != null) {
      if (channelFuture.isSuccess() && channelFuture.getChannel().isConnected()) {
        return new SimpleConnectResult(address, channelFuture);
      }
      channelFuture = channelFutures.poll();
    }

    channelFuture = bootstrap.connect(address);
    channelFuture.addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
        if (future.isSuccess()) {
          channelGroup.add(future.getChannel());
        }
      }
    });
    return new SimpleConnectResult(address, channelFuture);
  }

  ChannelGroupFuture close() {
    ChannelGroupFuture result = channelGroup.close();
    result.addListener(new ChannelGroupFutureListener() {
      @Override
      public void operationComplete(ChannelGroupFuture future) throws Exception {
        bootstrap.releaseExternalResources();
      }
    });
    return result;
  }

  private final class SimpleConnectResult implements ConnectResult {

    private final InetSocketAddress address;
    private final ChannelFuture future;


    private SimpleConnectResult(InetSocketAddress address, ChannelFuture future) {
      this.address = address;
      this.future = future;
    }

    @Override
    public ChannelFuture getChannelFuture() {
      return future;
    }

    @Override
    public void release() {
      if (future.isSuccess()) {
        connections.get(address).offer(future);
      }
    }
  }
}
