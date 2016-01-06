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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.io.LineReader;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Test for ability to discover existence of services through {@link TwillContext}.
 */
public final class ServiceDiscoveryTestRun extends BaseYarnTest {

  @Test
  public void testServiceDiscovery() throws InterruptedException, ExecutionException, TimeoutException {
    TwillRunner twillRunner = getTwillRunner();
    TwillController controller = twillRunner
      .prepare(new ServiceApplication())
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .withApplicationArguments("echo")
      .start();

    ServiceDiscovered discovered = controller.discoverService("discovered");
    Assert.assertTrue(waitForSize(discovered, 1, 120));
    controller.terminate().get();
  }

  /**
   * An application that contains an EchoServer and an EchoClient.
   */
  public static final class ServiceApplication implements TwillApplication {

    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
        .setName("ServiceApp")
        .withRunnable()
          .add("server", new EchoServer()).noLocalFiles()
          .add("client", new EchoClient()).noLocalFiles()
        .anyOrder()
        .build();
    }
  }

  /**
   * A runnable to discover the echo server and issue a call to it.
   */
  public static final class EchoClient extends AbstractTwillRunnable {

    private static final Logger LOG = LoggerFactory.getLogger(EchoClient.class);

    private final CountDownLatch completion = new CountDownLatch(1);

    @Override
    public void run() {
      final BlockingQueue<Discoverable> discoverables = new LinkedBlockingQueue<>();
      ServiceDiscovered serviceDiscovered = getContext().discover(getContext().getApplicationArguments()[0]);
      serviceDiscovered.watchChanges(new ServiceDiscovered.ChangeListener() {
        @Override
        public void onChange(ServiceDiscovered serviceDiscovered) {
          Iterables.addAll(discoverables, serviceDiscovered);
        }
      }, Threads.SAME_THREAD_EXECUTOR);

      try {
        Discoverable discoverable = discoverables.poll(120, TimeUnit.SECONDS);
        // Make a call to the echo server
        InetSocketAddress address = discoverable.getSocketAddress();
        Socket socket = new Socket(address.getAddress(), address.getPort());
        String message = "Hello World";
        try (PrintWriter printer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8"))) {
          printer.println(message);
          printer.flush();

          try (Reader reader = new InputStreamReader(socket.getInputStream(), "UTF-8")) {
            LineReader lineReader = new LineReader(reader);
            String line = lineReader.readLine();
            Preconditions.checkState(message.equals(line), "Expected %s, got %s", message, line);
          }
        }

        Cancellable cancellable = getContext().announce("discovered", 12345);
        completion.await();
        cancellable.cancel();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted.", e);
      } catch (IOException e) {
        LOG.error("Failed to talk to server", e);
      }
    }

    @Override
    public void stop() {
      completion.countDown();
    }
  }
}
