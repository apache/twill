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
package org.apache.twill.yarn;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillContext;
import org.apache.twill.common.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

/**
 * Boilerplate for a server that announces itself and talks to clients through a socket.
 */
public abstract class SocketServer extends AbstractTwillRunnable {

  private static final Logger LOG = LoggerFactory.getLogger(SocketServer.class);

  protected volatile boolean running;
  protected ServerSocket serverSocket;
  protected Cancellable canceller;

  @Override
  public void initialize(TwillContext context) {
    super.initialize(context);
    running = true;
    try {
      serverSocket = new ServerSocket(0);
      LOG.info("Server started: " + serverSocket.getLocalSocketAddress() +
               ", id: " + context.getInstanceId() +
               ", count: " + context.getInstanceCount());

      // Announce with service names as specified in app arguments and runnable arguments
      final List<Cancellable> cancellables = new ArrayList<>();
      for (String[] args : new String[][] {context.getApplicationArguments(), context.getArguments()}) {
        if (args.length > 0) {
          cancellables.add(context.announce(args[0], serverSocket.getLocalPort()));
        }
      }
      canceller = new Cancellable() {
        @Override
        public void cancel() {
          for (Cancellable c : cancellables) {
            c.cancel();
          }
        }
      };
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void run() {
    try {
      while (running) {
        try (Socket socket = serverSocket.accept()) {
          BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), Charsets.UTF_8));
          PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);
          handleRequest(reader, writer);
        } catch (SocketException e) {
          LOG.info("Socket exception: " + e);
        }
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  @Override
  public void stop() {
    LOG.info("Stopping server");
    canceller.cancel();
    running = false;
    try {
      serverSocket.close();
    } catch (IOException e) {
      LOG.error("Exception while closing socket.", e);
      throw Throwables.propagate(e);
    }
    serverSocket = null;
  }

  @Override
  public void destroy() {
    try {
      if (serverSocket != null) {
        serverSocket.close();
      }
    } catch (IOException e) {
      LOG.error("Exception while closing socket.", e);
      throw Throwables.propagate(e);
    }
  }

  public abstract void handleRequest(BufferedReader reader, PrintWriter writer) throws Exception;
}
