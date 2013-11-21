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

import com.google.common.base.Charsets;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

import java.nio.ByteBuffer;

/**
 *
 */
final class KafkaRequestEncoder extends OneToOneEncoder {

  @Override
  protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
    if (!(msg instanceof KafkaRequest)) {
      return msg;
    }
    KafkaRequest req = (KafkaRequest) msg;
    ByteBuffer topic = Charsets.UTF_8.encode(req.getTopic());

    ChannelBuffer buffer = ChannelBuffers.dynamicBuffer(16 + topic.remaining() + req.getBody().readableBytes());
    int writerIdx = buffer.writerIndex();
    buffer.writerIndex(writerIdx + 4);    // Reserves 4 bytes for message length

    // Write out <REQUEST_TYPE>, <TOPIC_LENGTH>, <TOPIC>, <PARTITION>
    buffer.writeShort(req.getType().getId());
    buffer.writeShort(topic.remaining());
    buffer.writeBytes(topic);
    buffer.writeInt(req.getPartition());

    // Write out the size of the whole buffer (excluding the size field) at the beginning
    buffer.setInt(writerIdx, buffer.readableBytes() - 4 + req.getBody().readableBytes());

    ChannelBuffer buf = ChannelBuffers.wrappedBuffer(buffer, req.getBody());
    buf = buf.readBytes(buf.readableBytes());

    return buf;
  }
}
