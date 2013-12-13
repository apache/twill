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

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketException;
import java.nio.channels.ClosedChannelException;

/**
 *
 */
final class KafkaResponseDispatcher extends SimpleChannelHandler {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaResponseDispatcher.class);

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    Object attachment = ctx.getAttachment();
    if (e.getMessage() instanceof KafkaResponse && attachment instanceof ResponseHandler) {
      ((ResponseHandler) attachment).received((KafkaResponse) e.getMessage());
    } else {
      super.messageReceived(ctx, e);
    }
  }

  @Override
  public void writeRequested(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    if (e.getMessage() instanceof KafkaRequest) {
      ctx.setAttachment(((KafkaRequest) e.getMessage()).getResponseHandler());
    }
    super.writeRequested(ctx, e);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
    if (e.getCause() instanceof ClosedChannelException || e.getCause() instanceof SocketException) {
      // No need to log for socket exception as the client has logic to retry.
      return;
    }
    LOG.warn("Exception caught in kafka client connection.", e.getCause());
  }
}
