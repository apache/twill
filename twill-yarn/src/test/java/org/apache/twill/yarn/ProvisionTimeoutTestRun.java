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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.EventHandlerContext;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public final class ProvisionTimeoutTestRun extends BaseYarnTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  public static final String ABORTED_FILE = "aborted_file";

  @Test
  public void testProvisionTimeout() throws InterruptedException, ExecutionException, TimeoutException, IOException {
    TwillRunner runner = getTwillRunner();
    // Create a parent folder to be written by EventHandler#aborted()
    File parentFolder = TMP_FOLDER.newFolder();
    parentFolder.setWritable(true, false);
    TwillController controller = runner.prepare(new TimeoutApplication(parentFolder.getAbsolutePath()))
                                       .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
                                       .start();

    // The provision should failed in 30 seconds after AM started, which AM could took a while to start.
    // Hence we give 90 seconds max time here.
    try {
      controller.awaitTerminated(90, TimeUnit.SECONDS);
      // EventHandler#aborted() method should be called to create a file
      Assert.assertTrue(new File(parentFolder.getAbsolutePath(), ABORTED_FILE).exists());
      String[] abortedFiles = parentFolder.list();
      Assert.assertNotNull(abortedFiles);
      Assert.assertEquals(1, abortedFiles.length);
    } finally {
      // If it timeout, kill the app as cleanup.
      controller.kill();
    }
  }

  /**
   * The handler for testing timeout handling.
   */
  public static final class Handler extends EventHandler {

    private final String parentFolderPath;
    private boolean abort;

    public Handler(String parentFolderPath) {
      this.parentFolderPath = parentFolderPath;
    }

    @Override
    protected Map<String, String> getConfigs() {
      return ImmutableMap.of("abort", "true", "parentFolderPath", parentFolderPath);
    }

    @Override
    public void initialize(EventHandlerContext context) {
      super.initialize(context);
      this.abort = Boolean.parseBoolean(context.getSpecification().getConfigs().get("abort"));
    }

    @Override
    public TimeoutAction launchTimeout(Iterable<TimeoutEvent> timeoutEvents) {
      if (abort) {
        return TimeoutAction.abort();
      } else {
        return TimeoutAction.recheck(10, TimeUnit.SECONDS);
      }
    }

    @Override
    public void aborted() {
      try {
        new File(context.getSpecification().getConfigs().get("parentFolderPath"), ABORTED_FILE).createNewFile();
      } catch (IOException e) {
        Throwables.propagate(e);
      }
    }
  }

  /**
   * Testing application for timeout.
   */
  public static final class TimeoutApplication implements TwillApplication {

    private final String parentFolderPath;

    public TimeoutApplication(String parentFolderPath) {
      this.parentFolderPath = parentFolderPath;
    }

    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
        .setName("TimeoutApplication")
        .withRunnable()
        .add(new TimeoutRunnable(),
             ResourceSpecification.Builder.with()
               .setVirtualCores(1)
               .setMemory(8, ResourceSpecification.SizeUnit.GIGA).build())
        .noLocalFiles()
        .anyOrder()
        .withEventHandler(new Handler(parentFolderPath))
        .build();
    }
  }

  /**
   * A runnable that do nothing, as it's not expected to get provisioned.
   */
  public static final class TimeoutRunnable extends AbstractTwillRunnable {

    private final CountDownLatch latch = new CountDownLatch(1);

    @Override
    public void stop() {
      latch.countDown();
    }

    @Override
    public void run() {
      // Simply block here
      try {
        latch.await();
      } catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
    }
  }
}
