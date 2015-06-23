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
import com.google.common.collect.Sets;
import com.google.common.io.LineReader;
import org.apache.twill.api.Command;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.discovery.Discoverable;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public final class FailureRestartTestRun extends BaseYarnTest {

  @Test
  public void testFailureRestart() throws Exception {
    TwillRunner runner = getTwillRunner();

    ResourceSpecification resource = ResourceSpecification.Builder.with()
      .setVirtualCores(1)
      .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(2)
      .build();
    TwillController controller = runner.prepare(new FailureRunnable(), resource)
      .withApplicationArguments("failure")
      .withArguments(FailureRunnable.class.getSimpleName(), "failure2")
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .start();

    Iterable<Discoverable> discoverables = controller.discoverService("failure");
    Assert.assertTrue(waitForSize(discoverables, 2, 120));

    // Make sure we see the right instance IDs
    Assert.assertEquals(Sets.newHashSet(0, 1), getInstances(discoverables));

    // Kill server with instanceId = 0
    controller.sendCommand(FailureRunnable.class.getSimpleName(), Command.Builder.of("kill0").build());

    // Make sure the runnable is killed.
    Assert.assertTrue(waitForSize(discoverables, 1, 120));

    // Wait for the restart
    Assert.assertTrue(waitForSize(discoverables, 2, 120));

    // Make sure we see the right instance IDs
    Assert.assertEquals(Sets.newHashSet(0, 1), getInstances(discoverables));

    controller.terminate().get(120, TimeUnit.SECONDS);
  }

  private Set<Integer> getInstances(Iterable<Discoverable> discoverables) throws IOException {
    Set<Integer> instances = Sets.newHashSet();
    for (Discoverable discoverable : discoverables) {
      InetSocketAddress socketAddress = discoverable.getSocketAddress();
      try (Socket socket = new Socket(socketAddress.getAddress(), socketAddress.getPort())) {
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), Charsets.UTF_8), true);
        LineReader reader = new LineReader(new InputStreamReader(socket.getInputStream(), Charsets.UTF_8));

        String msg = "Failure";
        writer.println(msg);

        String line = reader.readLine();
        Assert.assertTrue(line.endsWith(msg));
        instances.add(Integer.parseInt(line.substring(0, line.length() - msg.length())));
      }
    }
    return instances;
  }


  /**
   * A SocketServer that fails upon receiving a kill command.
   */
  public static final class FailureRunnable extends SocketServer {

    private volatile boolean killed;

    @Override
    public void run() {
      killed = false;
      super.run();
      if (killed) {
        throw new RuntimeException("Exception");
      }
    }

    @Override
    public void handleCommand(Command command) throws Exception {
      if (command.getCommand().equals("kill" + getContext().getInstanceId())) {
        killed = true;
        running = false;
        serverSocket.close();
      }
    }

    @Override
    public void handleRequest(BufferedReader reader, PrintWriter writer) throws IOException {
      String line = reader.readLine();
      writer.println(getContext().getInstanceId() + line);
      writer.flush();
    }
  }
}
