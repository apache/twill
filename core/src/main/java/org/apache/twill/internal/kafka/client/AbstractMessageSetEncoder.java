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

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import java.util.zip.CRC32;

/**
 * A base implementation of {@link MessageSetEncoder}.
 */
abstract class AbstractMessageSetEncoder implements MessageSetEncoder {

  private static final ThreadLocal<CRC32> CRC32_LOCAL = new ThreadLocal<CRC32>() {
    @Override
    protected CRC32 initialValue() {
      return new CRC32();
    }
  };

  protected final int computeCRC32(ChannelBuffer buffer) {
    CRC32 crc32 = CRC32_LOCAL.get();
    crc32.reset();

    if (buffer.hasArray()) {
      crc32.update(buffer.array(), buffer.arrayOffset() + buffer.readerIndex(), buffer.readableBytes());
    } else {
      byte[] bytes = new byte[buffer.readableBytes()];
      buffer.getBytes(buffer.readerIndex(), bytes);
      crc32.update(bytes);
    }
    return (int) crc32.getValue();
  }

  protected final ChannelBuffer encodePayload(ChannelBuffer payload) {
    return encodePayload(payload, Compression.NONE);
  }

  protected final ChannelBuffer encodePayload(ChannelBuffer payload, Compression compression) {
    ChannelBuffer header = ChannelBuffers.buffer(10);

    int crc = computeCRC32(payload);

    int magic = ((compression == Compression.NONE) ? 0 : 1);

    // Message length = 1 byte magic + (optional 1 compression byte) + 4 bytes crc + payload length
    header.writeInt(5 + magic + payload.readableBytes());
    // Magic number = 0 for non-compressed data
    header.writeByte(magic);
    if (magic > 0) {
      header.writeByte(compression.getCode());
    }
    header.writeInt(crc);

    return ChannelBuffers.wrappedBuffer(header, payload);
  }

  protected final ChannelBuffer prefixLength(ChannelBuffer buffer) {
    ChannelBuffer sizeBuf = ChannelBuffers.buffer(4);
    sizeBuf.writeInt(buffer.readableBytes());
    return ChannelBuffers.wrappedBuffer(sizeBuf, buffer);
  }
}
