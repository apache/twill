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
package org.apache.twill.discovery;

import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;

/**
 * Test for the {@link Discoverable} object.
 */
public class DiscoverableTest {

  @Test
  public void testEqualsAndHashCode() {
    Discoverable d1 = new Discoverable("service1", InetSocketAddress.createUnresolved("host1", 12345),
                                       new byte[] {1, 2, 3, 4});
    Discoverable d2 = new Discoverable("service1", InetSocketAddress.createUnresolved("host1", 12345),
                                       new byte[] {1, 2, 3, 4});
    Assert.assertEquals(d1, d2);
    Assert.assertEquals(d1.hashCode(), d2.hashCode());

    Discoverable d3 = new Discoverable("service2", InetSocketAddress.createUnresolved("host1", 12345),
                                       new byte[] {1, 2, 3, 4});
    Assert.assertNotEquals(d1, d3);
    Assert.assertNotEquals(d1.hashCode(), d3.hashCode());

    Discoverable d4 = new Discoverable("service1", InetSocketAddress.createUnresolved("host2", 12345),
                                       new byte[] {1, 2, 3, 4});
    Assert.assertNotEquals(d1, d4);
    Assert.assertNotEquals(d1.hashCode(), d4.hashCode());

    Discoverable d5 = new Discoverable("service1", InetSocketAddress.createUnresolved("host1", 12345));
    Assert.assertNotEquals(d1, d5);
    Assert.assertNotEquals(d1.hashCode(), d5.hashCode());
  }
}
