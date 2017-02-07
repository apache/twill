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
package org.apache.twill.internal;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

/**
 * Unit test for {@link org.apache.twill.internal.JvmOptions.DebugOptions} class.
 */
public class DebugOptionsTest {

  @Test
  public void testNoDebug() {
    JvmOptions.DebugOptions noDebug = new JvmOptions.DebugOptions(false, false, null);

    Assert.assertEquals(JvmOptions.DebugOptions.NO_DEBUG, noDebug);
  }

  @Test
  public void testWithNull() {
    JvmOptions.DebugOptions noDebug = new JvmOptions.DebugOptions(false, false, null);

    Assert.assertNotEquals(noDebug, null);
  }

  @Test
  public void testDoDebug() {
    JvmOptions.DebugOptions option1 = new JvmOptions.DebugOptions(true, false, null);

    Assert.assertNotEquals(JvmOptions.DebugOptions.NO_DEBUG, option1);

    JvmOptions.DebugOptions option2 = new JvmOptions.DebugOptions(true, false, null);

    Assert.assertEquals(option1, option2);
  }

  @Test
  public void testDoSuspend() {
    JvmOptions.DebugOptions option1 = new JvmOptions.DebugOptions(true, true, null);

    Assert.assertNotEquals(JvmOptions.DebugOptions.NO_DEBUG, option1);

    JvmOptions.DebugOptions option2 = new JvmOptions.DebugOptions(true, true, null);

    Assert.assertEquals(option1, option2);

  }

  @Test
  public void testSameRunnables() {
    Set<String> runnables = Sets.newHashSet();
    runnables.add("runnable1");
    runnables.add("runnable2");

    JvmOptions.DebugOptions option1 = new JvmOptions.DebugOptions(false, false, runnables);

    JvmOptions.DebugOptions option2 = new JvmOptions.DebugOptions(false, false, runnables);

    Assert.assertEquals(option1, option2);
  }

  @Test
  public void testDifferentRunnables() {
    Set<String> runnables1 = Sets.newHashSet();
    runnables1.add("runnable1");

    JvmOptions.DebugOptions option1 = new JvmOptions.DebugOptions(true, false, runnables1);

    Assert.assertNotEquals(option1, JvmOptions.DebugOptions.NO_DEBUG);

    Set<String> runnables2 = Sets.newHashSet();
    runnables2.add("runnable2-1");
    runnables2.add("runnable2-2");

    JvmOptions.DebugOptions option2 = new JvmOptions.DebugOptions(true, false, runnables2);

    Assert.assertNotEquals(option1, option2);

  }
}
