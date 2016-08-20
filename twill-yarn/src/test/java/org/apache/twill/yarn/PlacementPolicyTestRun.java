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

import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.twill.api.Hosts;
import org.apache.twill.api.Racks;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.discovery.ServiceDiscovered;
import org.apache.twill.internal.yarn.YarnUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tests for placement Policies.
 */
public class PlacementPolicyTestRun extends BaseYarnTest {
  private static final Logger LOG = LoggerFactory.getLogger(PlacementPolicyTestRun.class);

  private static final int RUNNABLE_MEMORY = 512;
  private static final int RUNNABLE_CORES = 1;

  private static List<NodeReport> nodeReports;
  private static ResourceSpecification resource;
  private static ResourceSpecification twoInstancesResource;

  /**
   * Verify the cluster configuration (number and capability of node managers) required for the tests.
   */
  @BeforeClass
  public static void verifyClusterCapability() throws InterruptedException {
    // Ignore verifications if it is running against older Hadoop versions which does not support blacklists.
    Assume.assumeTrue(YarnUtils.getHadoopVersion().equals(YarnUtils.HadoopVersions.HADOOP_22));

    // All runnables in this test class use same resource specification for the sake of convenience.
    resource = ResourceSpecification.Builder.with()
      .setVirtualCores(RUNNABLE_CORES)
      .setMemory(RUNNABLE_MEMORY, ResourceSpecification.SizeUnit.MEGA)
      .build();
    twoInstancesResource = ResourceSpecification.Builder.with()
      .setVirtualCores(RUNNABLE_CORES)
      .setMemory(RUNNABLE_MEMORY, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(2)
      .build();

    // The tests need exactly three NodeManagers in the cluster.
    int trials = 0;
    while (trials++ < 20) {
      try {
        nodeReports = TWILL_TESTER.getNodeReports();
        if (nodeReports != null && nodeReports.size() == 3) {
          break;
        }
      } catch (Exception e) {
        LOG.error("Failed to get node reports", e);
      }
      LOG.warn("NodeManagers != 3. {}", nodeReports);
      TimeUnit.SECONDS.sleep(1);
    }

    // All NodeManagers should have enough capacity available to accommodate at least two runnables.
    for (NodeReport nodeReport : nodeReports) {
      Resource capability = nodeReport.getCapability();
      Resource used = nodeReport.getUsed();
      Assert.assertNotNull(capability);
      if (used != null) {
        Assert.assertTrue(2 * resource.getMemorySize() < capability.getMemory() - used.getMemory());
      } else {
        Assert.assertTrue(2 * resource.getMemorySize() < capability.getMemory());
      }
    }
  }

  /**
   * Test to verify placement policy without dynamically changing number of instances.
   */
  @Test
  public void testPlacementPolicy() throws Exception {
    // Ignore test if it is running against older Hadoop versions which does not support blacklists.
    Assume.assumeTrue(YarnUtils.getHadoopVersion().equals(YarnUtils.HadoopVersions.HADOOP_22));

    waitNodeManagerCount(0, 10, TimeUnit.SECONDS);

    TwillRunner runner = getTwillRunner();
    TwillController controller = runner.prepare(new PlacementPolicyApplication())
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .withApplicationArguments("PlacementPolicyTest")
      .withArguments("hostRunnable", "host")
      .withArguments("hostRackRunnable", "hostRack")
      .withArguments("distributedRunnable", "distributed")
      .start();

    try {
      // All runnables should get started.
      ServiceDiscovered serviceDiscovered = controller.discoverService("PlacementPolicyTest");
      Assert.assertTrue(waitForSize(serviceDiscovered, 4, 80));

      // DISTRIBUTED runnables should be provisioned on different nodes.
      Assert.assertTrue(getProvisionedNodeManagerCount() >= 2);
    } finally {
      controller.terminate().get(120, TimeUnit.SECONDS);
    }

    // Sleep a bit before exiting.
    TimeUnit.SECONDS.sleep(2);
  }

  /**
   * An application that specify runnables with different placement policies.
   */
  public static final class PlacementPolicyApplication implements TwillApplication {

    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
        .setName("PlacementPolicyApplication")
        .withRunnable()
          .add("hostRunnable", new EchoServer(), resource).noLocalFiles()
          .add("hostRackRunnable", new EchoServer(), resource).noLocalFiles()
          .add("distributedRunnable", new EchoServer(), twoInstancesResource).noLocalFiles()
        .withPlacementPolicy()
          .add(Hosts.of(nodeReports.get(0).getHttpAddress()), "hostRunnable")
          .add(Hosts.of(nodeReports.get(1).getHttpAddress()), Racks.of("/default-rack"), "hostRackRunnable")
          .add(TwillSpecification.PlacementPolicy.Type.DISTRIBUTED, "distributedRunnable")
        .anyOrder()
        .build();
    }
  }

  /**
   * Test to verify DISTRIBUTED placement policies are taken care of when number of instances are changed.
   * Also, verifies that DISTRIBUTED placement policies do not affect other runnables.
   */
  @Test
  public void testDistributedPlacementPolicy() throws Exception {
    // Ignore test if it is running against older Hadoop versions which does not support blacklists.
    Assume.assumeTrue(YarnUtils.getHadoopVersion().equals(YarnUtils.HadoopVersions.HADOOP_22));

    waitNodeManagerCount(0, 10, TimeUnit.SECONDS);

    TwillRunner runner = getTwillRunner();
    TwillController controller = runner.prepare(new DistributedApplication())
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .withApplicationArguments("DistributedTest")
      .withArguments("Alice", "alice")
      .withArguments("Bob", "bob")
      .withArguments("Eve", "eve")
      .start();

    try {
      // All runnables should get started with DISTRIBUTED ones being on different nodes.
      ServiceDiscovered serviceDiscovered = controller.discoverService("DistributedTest");
      Assert.assertTrue(waitForSize(serviceDiscovered, 3, 60));
      Assert.assertTrue(getProvisionedNodeManagerCount() >= 2);

      // Spawning a new instance for DISTRIBUTED runnable Alice, which should get a different node.
      controller.changeInstances("Alice", 2).get(60, TimeUnit.SECONDS);
      serviceDiscovered = controller.discoverService("DistributedTest");
      Assert.assertTrue(waitForSize(serviceDiscovered, 4, 60));
      Assert.assertTrue(getProvisionedNodeManagerCount() >= 3);

      // Spawning a new instance for DEFAULT runnable Eve,
      // which should not be affected by placement policies of previous runnables.
      controller.changeInstances("Eve", 2).get(60, TimeUnit.SECONDS);
      serviceDiscovered = controller.discoverService("DistributedTest");
      Assert.assertTrue(waitForSize(serviceDiscovered, 5, 60));

      // Spawning a new instance for DISTRIBUTED runnable Bob,
      // which will be forced to give up it's placement policy restrictions, since there are only three nodes.
      controller.changeInstances("Bob", 2).get(60, TimeUnit.SECONDS);
      serviceDiscovered = controller.discoverService("DistributedTest");
      Assert.assertTrue(waitForSize(serviceDiscovered, 6, 60));
      Assert.assertTrue(getProvisionedNodeManagerCount() >= 3);
    } finally {
      controller.terminate().get(120, TimeUnit.SECONDS);
    }

    // Sleep a bit before exiting.
    TimeUnit.SECONDS.sleep(2);
  }

  private void waitNodeManagerCount(int expected, long timeout, TimeUnit unit) throws Exception {
    int count = getProvisionedNodeManagerCount();
    long startTime = System.currentTimeMillis();
    long elapse = 0L;

    while (count != expected && elapse < unit.toMillis(timeout)) {
      LOG.info("Waiting for expected number of node managers. Expected: {}. Actual: {}", expected, count);
      TimeUnit.SECONDS.sleep(1);
      count = getProvisionedNodeManagerCount();
      elapse = System.currentTimeMillis() - startTime;
    }
    if (count != expected) {
      throw new TimeoutException("Failed to get expected number of node managers. " +
                                 "Expected: " + expected + ". Actual: " + count);
    }
  }

  /**
   * An application that runs three runnables, with a DISTRIBUTED placement policy for two of them.
   */
  public static final class DistributedApplication implements TwillApplication {

    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
        .setName("DistributedApplication")
        .withRunnable()
          .add("Alice", new EchoServer(), resource).noLocalFiles()
          .add("Bob", new EchoServer(), resource).noLocalFiles()
          .add("Eve", new EchoServer(), resource).noLocalFiles()
        .withPlacementPolicy()
          .add(TwillSpecification.PlacementPolicy.Type.DISTRIBUTED, "Alice", "Bob")
        .anyOrder()
        .build();
    }
  }

  /**
   * Test to verify changing instances during application run works for DISTRIBUTED runnables.
   */
  @Test
  public void testChangeInstance() throws InterruptedException, TimeoutException, ExecutionException {
    // Ignore test if it is running against older Hadoop versions which does not support blacklists.
    Assume.assumeTrue(YarnUtils.getHadoopVersion().equals(YarnUtils.HadoopVersions.HADOOP_22));

    ServiceDiscovered serviceDiscovered;

    TwillRunner runner = getTwillRunner();
    TwillController controller = runner.prepare(new ChangeInstanceApplication())
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .withApplicationArguments("DistributedTest")
      .withArguments("Alice", "alice")
      .withArguments("Bob", "bob")
      .withArguments("Eve", "eve")
      .start();

    try {
      // All runnables should get started.
      serviceDiscovered = controller.discoverService("DistributedTest");
      Assert.assertTrue(waitForSize(serviceDiscovered, 4, 60));

      // Increasing the instance count for runnable Alice by 2.
      controller.changeInstances("Alice", 4).get(60, TimeUnit.SECONDS);
      serviceDiscovered = controller.discoverService("DistributedTest");
      Assert.assertTrue(waitForSize(serviceDiscovered, 6, 60));

      // Decreasing instance count for runnable Alice by 3.
      controller.changeInstances("Alice", 1).get(60, TimeUnit.SECONDS);
      serviceDiscovered = controller.discoverService("DistributedTest");
      Assert.assertTrue(waitForSize(serviceDiscovered, 3, 60));

      // Increasing instance count for runnable Bob by 2.
      controller.changeInstances("Bob", 3).get(60, TimeUnit.SECONDS);
      serviceDiscovered = controller.discoverService("DistributedTest");
      Assert.assertTrue(waitForSize(serviceDiscovered, 5, 60));

      // Increasing instance count for runnable Eve by 2.
      controller.changeInstances("Eve", 3).get(60, TimeUnit.SECONDS);
      serviceDiscovered = controller.discoverService("DistributedTest");
      Assert.assertTrue(waitForSize(serviceDiscovered, 7, 60));
    } finally {
      controller.terminate().get(120, TimeUnit.SECONDS);
    }

    // Sleep a bit before exiting.
    TimeUnit.SECONDS.sleep(2);
  }

  /**
   * An application that runs three runnables, with a DISTRIBUTED placement policy for two of them.
   */
  public static final class ChangeInstanceApplication implements TwillApplication {

    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
        .setName("DistributedApplication")
        .withRunnable()
        .add("Alice", new EchoServer(), twoInstancesResource).noLocalFiles()
        .add("Bob", new EchoServer(), resource).noLocalFiles()
        .add("Eve", new EchoServer(), resource).noLocalFiles()
        .withPlacementPolicy()
        .add(TwillSpecification.PlacementPolicy.Type.DISTRIBUTED, "Alice", "Bob")
        .anyOrder()
        .build();
    }
  }

  /**
   * Test to verify exception is thrown in case a non-existent runnable is specified in a placement policy.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testNonExistentRunnable() throws InterruptedException, ExecutionException, TimeoutException {
    TwillRunner runner = getTwillRunner();
    TwillController controller = runner.prepare(new FaultyApplication())
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .start();
    controller.terminate().get(120, TimeUnit.SECONDS);
  }

  /**
   * An application that uses non-existent runnable name while specifying placement policies.
   */
  public static final class FaultyApplication implements TwillApplication {

    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
        .setName("FaultyApplication")
        .withRunnable()
          .add("Hermione", new EchoServer(), resource).noLocalFiles()
          .add("Harry", new EchoServer(), resource).noLocalFiles()
          .add("Ron", new EchoServer(), resource).noLocalFiles()
        .withPlacementPolicy()
          .add(TwillSpecification.PlacementPolicy.Type.DEFAULT, "Hermione", "Ron")
          .add(TwillSpecification.PlacementPolicy.Type.DISTRIBUTED, "Draco", "Harry")
          .anyOrder()
        .build();
    }
  }

  /**
   * Test to verify exception is thrown in case a runnable is mentioned in more than one placement policy.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testPlacementPolicySpecification() throws InterruptedException, ExecutionException, TimeoutException {
    TwillRunner runner = getTwillRunner();
    TwillController controller = runner.prepare(new BadApplication())
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .start();
    controller.terminate().get(120, TimeUnit.SECONDS);
  }

  /**
   * An application that specifies a runnable name in more than one placement policy.
   */
  public static final class BadApplication implements TwillApplication {

    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
        .setName("BadApplication")
        .withRunnable()
          .add("Hermione", new EchoServer(), resource).noLocalFiles()
          .add("Harry", new EchoServer(), resource).noLocalFiles()
          .add("Ron", new EchoServer(), resource).noLocalFiles()
        .withPlacementPolicy()
          .add(TwillSpecification.PlacementPolicy.Type.DEFAULT, "Hermione", "Harry")
          .add(TwillSpecification.PlacementPolicy.Type.DISTRIBUTED, "Hermione", "Ron")
        .anyOrder()
        .build();
    }
  }

  /**
   * Helper function to verify DISTRIBUTED placement policies.
   * Returns the number of NodeManagers on which runnables got provisioned.
   * @return number of NodeManagers on which runnables got provisioned.
   */
  private int getProvisionedNodeManagerCount() throws Exception {
    int provisionedNodeManagerCount = 0;
    for (NodeReport nodeReport : getNodeReports()) {
      Resource used = nodeReport.getUsed();
      if (used != null && used.getMemory() > 0) {
          provisionedNodeManagerCount++;
      }
    }
    return provisionedNodeManagerCount;
  }
}
