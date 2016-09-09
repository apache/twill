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

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Objects;

/**
 * Discoverable defines the attributes of service to be discovered.
 */
public class Discoverable {
  private final String name;
  private final InetSocketAddress address;
  private final byte[] payload;

  public Discoverable(String name, InetSocketAddress address, byte[] payload) {
    this.name = name;
    this.address = address;
    this.payload = payload;
  }

  public Discoverable(String name, InetSocketAddress address) {
    this(name, address, new byte[]{});
  }

  /**
   * @return Name of the service
   */
  public String getName() {
    return name;
  }

  /**
   * @return An {@link InetSocketAddress} representing the host+port of the service.
   */
  public InetSocketAddress getSocketAddress() {
    return address;
  }

  /**
   * @return A payload represented as a byte array
   */
  public byte[] getPayload() {
    return payload;
  }

  @Override
  public String toString() {
    return "{name=" + name + ", address=" + address + ", payload=" + payload + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Discoverable other = (Discoverable) o;

    return name.equals(other.getName()) && address.equals(other.getSocketAddress()) &&
      Arrays.equals(payload, other.getPayload());
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, address, payload);
  }
}
