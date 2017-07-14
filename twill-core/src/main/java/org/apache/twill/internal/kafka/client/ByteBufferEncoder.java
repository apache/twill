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
package org.apache.twill.internal.kafka.client;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;

/**
 * A kafka {@link kafka.serializer.Encoder} for encoding byte buffer into byte array.
 */
public final class ByteBufferEncoder implements Encoder<ByteBuffer> {

  public ByteBufferEncoder(VerifiableProperties properties) {
  }

  public byte[] toBytes(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return bytes;
  }
}
