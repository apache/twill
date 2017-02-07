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

import org.apache.twill.kafka.client.FetchedMessage;
import org.apache.twill.kafka.client.TopicPartition;

import java.nio.ByteBuffer;

/**
 * An implementation of FetchedMessage that provides setters as well.
 */
final class BasicFetchedMessage implements FetchedMessage {

  private final TopicPartition topicPartition;
  private ByteBuffer payload;
  private long offset;
  private long nextOffset;

  BasicFetchedMessage(TopicPartition topicPartition) {
    this.topicPartition = topicPartition;
  }

  void setPayload(ByteBuffer payload) {
    this.payload = payload;
  }

  void setOffset(long offset) {
    this.offset = offset;
  }

  void setNextOffset(long nextOffset) {
    this.nextOffset = nextOffset;
  }

  @Override
  public TopicPartition getTopicPartition() {
    return topicPartition;
  }

  @Override
  public ByteBuffer getPayload() {
    return payload;
  }

  @Override
  public long getOffset() {
    return offset;
  }

  @Override
  public long getNextOffset() {
    return nextOffset;
  }
}
