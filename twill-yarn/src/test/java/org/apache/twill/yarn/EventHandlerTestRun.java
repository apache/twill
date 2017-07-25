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

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tests {@link EventHandler} methods
 */
public final class EventHandlerTestRun extends BaseYarnTest {
  private static final Logger LOG = LoggerFactory.getLogger(EventHandlerTestRun.class);

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  public static final String PARENT_FOLDER = "parent_folder";
  public static final String STARTED_FILE = "started_file";
  public static final String RUN_FILE = "run_file";
  public static final String CONTAINER_LAUNCHED_FOLDER = "launched_folder";
  public static final String CONTAINER_STOPPED_FOLDER = "stopped_folder";
  public static final String COMPLETED_FILE = "completed_file";
  public static final String KILLED_FILE = "killed_file";
  public static final String ABORTED_FILE = "aborted_file";

  @Test
  public void testComplete() throws InterruptedException, ExecutionException, TimeoutException, IOException {
    // Create a parent folder to be written by EventHandler
    File parentFolder = TMP_FOLDER.newFolder();
    parentFolder.setWritable(true, false);
    TwillController controller = getTwillRunner().prepare(new CompleteApplication(parentFolder.getAbsolutePath()))
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .withApplicationArguments(parentFolder.getAbsolutePath())
      .start();

    // Wait for the app to complete within 120 seconds.
    controller.awaitTerminated(120, TimeUnit.SECONDS);
    // EventHandler#started() method should be called to create a file
    Assert.assertTrue(new File(parentFolder.getAbsolutePath(), STARTED_FILE).exists());
    // CompleteRunnable#run() method should be called to create a file after EventHandler#started() method is called
    Assert.assertTrue(new File(parentFolder.getAbsolutePath(), RUN_FILE).exists());
    // EventHandler#containerLaunched(String, int, String) method should be called to create a folder
    Assert.assertTrue(new File(parentFolder.getAbsolutePath(), CONTAINER_LAUNCHED_FOLDER).exists());
    // EventHandler#containerStopped(String, int, String, int) method should be called to create a folder
    Assert.assertTrue(new File(parentFolder.getAbsolutePath(), CONTAINER_STOPPED_FOLDER).exists());
    // EventHandler#completed() method should be called to create a file
    Assert.assertTrue(new File(parentFolder.getAbsolutePath(), COMPLETED_FILE).exists());
    // EventHandler#killed() method should not be called
    Assert.assertFalse(new File(parentFolder.getAbsolutePath(), KILLED_FILE).exists());
    // EventHandler#aborted() method should not be called
    Assert.assertFalse(new File(parentFolder.getAbsolutePath(), ABORTED_FILE).exists());
    // Assert that containerLaunched and containerStopped are called for the same containers
    // for the same number of times
    String[] containerLaunchedFiles = new File(parentFolder.getAbsolutePath(), CONTAINER_LAUNCHED_FOLDER).list();
    String[] containerStoppedFiles = new File(parentFolder.getAbsolutePath(), CONTAINER_STOPPED_FOLDER).list();
    Assert.assertEquals(containerLaunchedFiles.length, containerStoppedFiles.length);
    Assert.assertTrue(Arrays.asList(containerLaunchedFiles).containsAll(Arrays.asList(containerStoppedFiles)));
  }

  @Test
  public void testKilled() throws IOException, InterruptedException, TimeoutException, ExecutionException {
    // Create a parent folder to be written by EventHandler
    File parentFolder = TMP_FOLDER.newFolder();
    parentFolder.setWritable(true, false);
    TwillController controller = getTwillRunner().prepare(new SleepApplication(parentFolder.getAbsolutePath()))
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .start();
    // Wait for the runnable to run and create runFile within 120 secs
    File runFile = new File(parentFolder, RUN_FILE);
    Stopwatch stopwatch = new Stopwatch().start();
    while (!runFile.exists() && stopwatch.elapsedTime(TimeUnit.SECONDS) < 120) {
      TimeUnit.SECONDS.sleep(1);
    }
    Assert.assertTrue(runFile.exists());
    // Terminate the app once the runnable runs
    controller.terminate();
    controller.awaitTerminated(120, TimeUnit.SECONDS);
    // EventHandler#killed() method should be called to create a file
    Assert.assertTrue(new File(parentFolder.getAbsolutePath(), KILLED_FILE).exists());
    // EventHandler#completed() method should not be called
    Assert.assertFalse(new File(parentFolder.getAbsolutePath(), COMPLETED_FILE).exists());
    // EventHandler#aborted() method should not be called
    Assert.assertFalse(new File(parentFolder.getAbsolutePath(), ABORTED_FILE).exists());
  }

  private static void createFile(String parentPath, String childPath) {
    try {
      new File(parentPath, childPath).createNewFile();
    } catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  /**
   * The handler for testing timeout handling.
   */
  public static final class Handler extends EventHandler {

    private String parentFolderPath;

    public Handler(String parentFolderPath) {
      this.parentFolderPath = parentFolderPath;
    }

    @Override
    protected Map<String, String> getConfigs() {
      return ImmutableMap.of(PARENT_FOLDER, parentFolderPath);
    }

    @Override
    public void started() {
      createFile(context.getSpecification().getConfigs().get(PARENT_FOLDER), STARTED_FILE);
    }

    @Override
    public void containerLaunched(String runnableName, int instanceId, String containerId) {
      LOG.info("Launched {}#{} in container {}", runnableName, instanceId, containerId);
      createContainerFile(runnableName, instanceId, containerId, CONTAINER_LAUNCHED_FOLDER);
    }

    @Override
    public void containerStopped(String runnableName, int instanceId, String containerId, int exitStatus) {
      LOG.info("Stopped {}#{} in container {} with status {}", runnableName, instanceId, containerId, exitStatus);
      createContainerFile(runnableName, instanceId, containerId, CONTAINER_STOPPED_FOLDER);
    }

    private void createContainerFile(String runnableName, int instanceId, String containerId, String childFolder) {
      File launchedFolder = new File(context.getSpecification().getConfigs().get(PARENT_FOLDER), childFolder);
      if (!launchedFolder.exists()) {
        launchedFolder.mkdirs();
        launchedFolder.setReadable(true, false);
      }
      createFile(launchedFolder.getAbsolutePath(), Joiner.on(":").join(runnableName, instanceId, containerId));
    }

    @Override
    public void completed() {
      createFile(context.getSpecification().getConfigs().get(PARENT_FOLDER), COMPLETED_FILE);
    }

    @Override
    public void killed() {
      createFile(context.getSpecification().getConfigs().get(PARENT_FOLDER), KILLED_FILE);
    }

    @Override
    public void aborted() {
      createFile(context.getSpecification().getConfigs().get(PARENT_FOLDER), ABORTED_FILE);
    }
  }

  /**
   * Testing application with completed run.
   */
  public static final class CompleteApplication implements TwillApplication {

    private final String parentFolderPath;

    public CompleteApplication(String parentFolderPath) {
      this.parentFolderPath = parentFolderPath;
    }

    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
        .setName("CompleteApplication")
        .withRunnable()
        .add(new CompleteRunnable())
        .noLocalFiles()
        .anyOrder()
        .withEventHandler(new Handler(parentFolderPath))
        .build();
    }
  }

  /**
   * A runnable that creates a file in run method only if the file created by {@link Handler#started()} exists.
   */
  public static final class CompleteRunnable extends AbstractTwillRunnable {

    @Override
    public void run() {
      try {
        File startedFile = new File(getContext().getApplicationArguments()[0], STARTED_FILE);
        // CompleteRunnable#run() method should be called after EventHandler#started() method is called
        if (startedFile.exists()) {
          new File(startedFile.getParent(), RUN_FILE).createNewFile();
        }
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  /**
   * Application with a runnable that sleeps.
   */
  public static final class SleepApplication implements TwillApplication {

    private final String parentFolderPath;

    public SleepApplication(String parentFolderPath) {
      this.parentFolderPath = parentFolderPath;
    }

    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
        .setName("SleepApplication")
        .withRunnable()
        .add(new SleepRunnable(parentFolderPath))
        .noLocalFiles()
        .anyOrder()
        .withEventHandler(new Handler(parentFolderPath))
        .build();
    }
  }

  /**
   * A runnable that sleeps in run method.
   */
  public static final class SleepRunnable extends AbstractTwillRunnable {

    private CountDownLatch stopLatch;

    public SleepRunnable() {
      this.stopLatch = new CountDownLatch(1);
    }

    public SleepRunnable(String parentFolderPath) {
      super(ImmutableMap.of(PARENT_FOLDER, parentFolderPath, "startedFile", STARTED_FILE,
                            "runFile", RUN_FILE));
    }

    @Override
    public void run() {
      try {
        createFile(getContext().getSpecification().getConfigs().get(PARENT_FOLDER),
                   getContext().getSpecification().getConfigs().get("runFile"));
        LOG.info("runFile created");
        stopLatch.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public void stop() {
      stopLatch.countDown();
    }
  }
}
