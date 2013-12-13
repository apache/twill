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

/**
 *
 */
final class KafkaRequest {

  public enum Type {
    PRODUCE(0),
    FETCH(1),
    MULTI_FETCH(2),
    MULTI_PRODUCE(3),
    OFFSETS(4);

    private final short id;

    private Type(int id) {
      this.id = (short) id;
    }

    public short getId() {
      return id;
    }
  }

  private final Type type;
  private final String topic;
  private final int partition;
  private final ChannelBuffer body;
  private final ResponseHandler responseHandler;


  public static KafkaRequest createProduce(String topic, int partition, ChannelBuffer body) {
    return new KafkaRequest(Type.PRODUCE, topic, partition, body, ResponseHandler.NO_OP);
  }

  public static KafkaRequest createFetch(String topic, int partition, ChannelBuffer body, ResponseHandler handler) {
    return new KafkaRequest(Type.FETCH, topic, partition, body, handler);
  }

  public static KafkaRequest createOffsets(String topic, int partition, ChannelBuffer body, ResponseHandler handler) {
    return new KafkaRequest(Type.OFFSETS, topic, partition, body, handler);
  }

  private KafkaRequest(Type type, String topic, int partition, ChannelBuffer body, ResponseHandler responseHandler) {
    this.type = type;
    this.topic = topic;
    this.partition = partition;
    this.body = body;
    this.responseHandler = responseHandler;
  }

  Type getType() {
    return type;
  }

  String getTopic() {
    return topic;
  }

  int getPartition() {
    return partition;
  }

  ChannelBuffer getBody() {
    return body;
  }

  ResponseHandler getResponseHandler() {
    return responseHandler;
  }
}
