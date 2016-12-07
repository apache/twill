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
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.google.common.io.LineReader;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.discovery.Discoverable;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 * Test for local file transfer.
 */
public final class LocalFileTestRun extends BaseYarnTest {

  @Test
  public void testLocalFile() throws Exception {
    // Generate a header and a footer files.
    File headerFile = tmpFolder.newFile("header.txt");
    File footerFile = tmpFolder.newFile("footer.txt");

    String headerMsg = "Header Message";
    String footerMsg = "Footer Message";

    Files.write(headerMsg, headerFile, StandardCharsets.UTF_8);
    Files.write(footerMsg, footerFile, StandardCharsets.UTF_8);

    TwillRunner runner = getTwillRunner();

    TwillController controller = runner.prepare(new LocalFileApplication(headerFile))
      .addJVMOptions(" -verbose:gc -Xloggc:gc.log -XX:+PrintGCDetails")
      .withApplicationArguments("local")
      .withArguments("LocalFileSocketServer", "local2")
      .withResources(footerFile.toURI())
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .start();

    Iterable<Discoverable> discoverables = controller.discoverService("local");
    Assert.assertTrue(waitForSize(discoverables, 1, 60));

    InetSocketAddress socketAddress = discoverables.iterator().next().getSocketAddress();
    try (Socket socket = new Socket(socketAddress.getAddress(), socketAddress.getPort())) {
      PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), Charsets.UTF_8), true);
      LineReader reader = new LineReader(new InputStreamReader(socket.getInputStream(), Charsets.UTF_8));

      String msg = "Local file test";
      writer.println(msg);
      Assert.assertEquals(headerMsg, reader.readLine());
      Assert.assertEquals(msg, reader.readLine());
      Assert.assertEquals(footerMsg, reader.readLine());
    }

    controller.terminate().get(120, TimeUnit.SECONDS);

    Assert.assertTrue(waitForSize(discoverables, 0, 60));

    TimeUnit.SECONDS.sleep(2);
  }

  /**
   * Application for testing local file transfer.
   */
  public static final class LocalFileApplication implements TwillApplication {

    private final File headerJar;

    public LocalFileApplication(File headerFile) throws Exception {
      // Create a jar file that contains the header.txt file inside.
      headerJar = tmpFolder.newFile("header.jar");
      try (JarOutputStream os = new JarOutputStream(new FileOutputStream(headerJar))) {
        os.putNextEntry(new JarEntry(headerFile.getName()));
        Files.copy(headerFile, os);
      }
    }

    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
        .setName("LocalFileApp")
        .withRunnable()
          .add(new LocalFileSocketServer())
            .withLocalFiles()
              .add("header", headerJar, true).apply()
        .anyOrder()
        .build();
    }
  }

  /**
   * SocketServer for testing local file transfer.
   */
  public static final class LocalFileSocketServer extends SocketServer {

    private static final Logger LOG = LoggerFactory.getLogger(LocalFileSocketServer.class);

    @Override
    public void handleRequest(BufferedReader reader, PrintWriter writer) throws Exception {
      // Verify there is a gc.log file locally
      Preconditions.checkState(new File("gc.log").exists());

      // Get the footer file from classloader. Since it was added as resources as well, it should be loadable from CL.
      URL footerURL = getClass().getClassLoader().getResource("footer.txt");
      Preconditions.checkState(footerURL != null, "Missing footer.txt file from classloader");

      LOG.info("handleRequest");
      // Read from the localized file
      writer.println(Files.readFirstLine(new File("header/header.txt"), Charsets.UTF_8));
      // Read from the request
      writer.println(reader.readLine());
      // Read from resource
      writer.println(Files.readFirstLine(new File(footerURL.toURI()), Charsets.UTF_8));
      LOG.info("Flushed response");
    }
  }
}
