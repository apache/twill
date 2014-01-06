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

import com.google.common.collect.ImmutableSet;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillSpecification;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 *
 */
public class TwillSpecificationTest {

  /**
   * Dummy for test.
   */
  public static final class DummyRunnable extends AbstractTwillRunnable {

    @Override
    public void stop() {
      // no-op
    }

    @Override
    public void run() {
      // no-op
    }
  }

  @Test
  public void testAnyOrder() {
    TwillSpecification spec =
      TwillSpecification.Builder.with()
        .setName("Testing")
        .withRunnable()
        .add("r1", new DummyRunnable()).noLocalFiles()
        .add("r2", new DummyRunnable()).noLocalFiles()
        .add("r3", new DummyRunnable()).noLocalFiles()
        .anyOrder()
        .build();

    Assert.assertEquals(3, spec.getRunnables().size());
    List<TwillSpecification.Order> orders = spec.getOrders();
    Assert.assertEquals(1, orders.size());
    Assert.assertEquals(ImmutableSet.of("r1", "r2", "r3"), orders.get(0).getNames());
  }

  @Test
  public void testOrder() {
    TwillSpecification spec =
      TwillSpecification.Builder.with()
        .setName("Testing")
        .withRunnable()
        .add("r1", new DummyRunnable()).noLocalFiles()
        .add("r2", new DummyRunnable()).noLocalFiles()
        .add("r3", new DummyRunnable()).noLocalFiles()
        .add("r4", new DummyRunnable()).noLocalFiles()
        .withOrder().begin("r1", "r2").nextWhenStarted("r3")
        .build();

    Assert.assertEquals(4, spec.getRunnables().size());
    List<TwillSpecification.Order> orders = spec.getOrders();
    Assert.assertEquals(3, orders.size());
    Assert.assertEquals(ImmutableSet.of("r1", "r2"), orders.get(0).getNames());
    Assert.assertEquals(ImmutableSet.of("r3"), orders.get(1).getNames());
    Assert.assertEquals(ImmutableSet.of("r4"), orders.get(2).getNames());
  }
}
