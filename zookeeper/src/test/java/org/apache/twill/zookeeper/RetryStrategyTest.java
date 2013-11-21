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
package org.apache.twill.zookeeper;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class RetryStrategyTest {

  @Test
  public void testNoRetry() {
    RetryStrategy strategy = RetryStrategies.noRetry();
    long startTime = System.currentTimeMillis();
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals(-1L, strategy.nextRetry(i, startTime, RetryStrategy.OperationType.CREATE, "/"));
    }
  }

  @Test
  public void testLimit() {
    RetryStrategy strategy = RetryStrategies.limit(10, RetryStrategies.fixDelay(1, TimeUnit.MILLISECONDS));
    long startTime = System.currentTimeMillis();
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals(1L, strategy.nextRetry(i, startTime, RetryStrategy.OperationType.CREATE, "/"));
    }
    Assert.assertEquals(-1L, strategy.nextRetry(11, startTime, RetryStrategy.OperationType.CREATE, "/"));
  }

  @Test
  public void testUnlimited() {
    RetryStrategy strategy = RetryStrategies.fixDelay(1, TimeUnit.MILLISECONDS);
    long startTime = System.currentTimeMillis();
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals(1L, strategy.nextRetry(i, startTime, RetryStrategy.OperationType.CREATE, "/"));
    }
    Assert.assertEquals(1L, strategy.nextRetry(100000, startTime, RetryStrategy.OperationType.CREATE, "/"));
  }

  @Test
  public void testExponential() {
    RetryStrategy strategy = RetryStrategies.exponentialDelay(1, 60000, TimeUnit.MILLISECONDS);
    long startTime = System.currentTimeMillis();
    for (int i = 1; i <= 16; i++) {
      Assert.assertEquals(1L << (i - 1), strategy.nextRetry(i, startTime, RetryStrategy.OperationType.CREATE, "/"));
    }
    for (int i = 60; i <= 80; i++) {
      Assert.assertEquals(60000, strategy.nextRetry(i, startTime, RetryStrategy.OperationType.CREATE, "/"));
    }
  }

  @Test
  public void testExponentialLimit() {
    RetryStrategy strategy = RetryStrategies.limit(99,
                                                   RetryStrategies.exponentialDelay(1, 60000, TimeUnit.MILLISECONDS));
    long startTime = System.currentTimeMillis();
    for (int i = 1; i <= 16; i++) {
      Assert.assertEquals(1L << (i - 1), strategy.nextRetry(i, startTime, RetryStrategy.OperationType.CREATE, "/"));
    }
    for (int i = 60; i <= 80; i++) {
      Assert.assertEquals(60000, strategy.nextRetry(i, startTime, RetryStrategy.OperationType.CREATE, "/"));
    }
    Assert.assertEquals(-1L, strategy.nextRetry(100, startTime, RetryStrategy.OperationType.CREATE, "/"));
  }

  @Test
  public void testTimeLimit() throws InterruptedException {
    RetryStrategy strategy = RetryStrategies.timeLimit(1, TimeUnit.SECONDS,
                                                       RetryStrategies.fixDelay(1, TimeUnit.MILLISECONDS));
    long startTime = System.currentTimeMillis();
    Assert.assertEquals(1L, strategy.nextRetry(1, startTime, RetryStrategy.OperationType.CREATE, "/"));
    TimeUnit.MILLISECONDS.sleep(1100);
    Assert.assertEquals(-1L, strategy.nextRetry(2, startTime, RetryStrategy.OperationType.CREATE, "/"));
  }
}
