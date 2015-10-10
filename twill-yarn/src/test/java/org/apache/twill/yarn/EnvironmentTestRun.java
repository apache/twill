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

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.LineReader;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for testing environment settings.
 */
public class EnvironmentTestRun extends BaseYarnTest {

  @Test
  public void testEnv() throws Exception {
    TwillRunner runner = getTwillRunner();

    TwillController controller = runner.prepare(new EchoApp())
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .withApplicationArguments("echo")
      .withArguments("echo1", "echo1")
      .withArguments("echo2", "echo2")
      .withEnv(ImmutableMap.of("GREETING", "Hello"))
      .withEnv("echo2", ImmutableMap.of("GREETING", "Hello2"))
      .start();

    // Service echo1 should returns "Hello" as greeting, echo2 should returns "Hello2"
    Map<String, String> runnableGreetings = ImmutableMap.of("echo1", "Hello", "echo2", "Hello2");
    for (Map.Entry<String, String> entry : runnableGreetings.entrySet()) {
      Discoverable discoverable = getDiscoverable(controller.discoverService(entry.getKey()), 60, TimeUnit.SECONDS);
      try (
        Socket socket = new Socket(discoverable.getSocketAddress().getAddress(),
                                   discoverable.getSocketAddress().getPort())
      ) {
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), Charsets.UTF_8), true);
        LineReader reader = new LineReader(new InputStreamReader(socket.getInputStream(), Charsets.UTF_8));

        writer.println("GREETING");
        Assert.assertEquals(entry.getValue(), reader.readLine());
      }
    }

    controller.terminate().get();
  }

  private Discoverable getDiscoverable(ServiceDiscovered serviceDiscovered,
                                       long timeout, TimeUnit unit) throws Exception {
    final SettableFuture<Discoverable> completion = SettableFuture.create();
    serviceDiscovered.watchChanges(new ServiceDiscovered.ChangeListener() {
      @Override
      public void onChange(ServiceDiscovered serviceDiscovered) {
        Iterator<Discoverable> itor = serviceDiscovered.iterator();
        if (itor.hasNext()) {
          completion.set(itor.next());
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);
    return completion.get(timeout, unit);
  }

  /**
   * Application to add two {@link EnvironmentEchoServer} for testing.
   */
  public static final class EchoApp implements TwillApplication {

    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
        .setName("EchoApp")
        .withRunnable()
          .add("echo1", new EnvironmentEchoServer()).noLocalFiles()
          .add("echo2", new EnvironmentEchoServer()).noLocalFiles()
        .anyOrder()
        .build();
    }
  }
}
