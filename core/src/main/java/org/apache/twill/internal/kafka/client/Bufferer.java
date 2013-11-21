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

/**
 * A class to help buffering data of format [len][payload-of-len].
 */
final class Bufferer {

  private ChannelBuffer currentBuffer = null;
  private int currentSize = -1;

  void apply(ChannelBuffer buffer) {
    currentBuffer = concatBuffer(currentBuffer, buffer);
  }

  /**
   * Returns the buffer if the buffer data is ready to be consumed,
   * otherwise return {@link ChannelBuffers#EMPTY_BUFFER}.
   */
  ChannelBuffer getNext() {
    if (currentSize < 0) {
      if (currentBuffer.readableBytes() < 4) {
        return ChannelBuffers.EMPTY_BUFFER;
      }
      currentSize = currentBuffer.readInt();
    }

    // Keep buffering if less then required number of bytes
    if (currentBuffer.readableBytes() < currentSize) {
      return ChannelBuffers.EMPTY_BUFFER;
    }

    ChannelBuffer result = currentBuffer.readSlice(currentSize);
    currentSize = -1;

    return result;
  }

  private ChannelBuffer concatBuffer(ChannelBuffer current, ChannelBuffer buffer) {
    return current == null ? buffer : ChannelBuffers.wrappedBuffer(current, buffer);
  }
}
