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
package org.apache.twill.internal.state;

import com.google.common.collect.ImmutableMap;
import org.apache.twill.api.Command;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 *
 */
public class MessageCodecTest {

  @Test
  public void testCodec() {
    Message message = MessageCodec.decode(MessageCodec.encode(new Message() {

      @Override
      public Type getType() {
        return Type.SYSTEM;
      }

      @Override
      public Scope getScope() {
        return Scope.APPLICATION;
      }

      @Override
      public String getRunnableName() {
        return null;
      }

      @Override
      public Command getCommand() {
        return new Command() {
          @Override
          public String getCommand() {
            return "stop";
          }

          @Override
          public Map<String, String> getOptions() {
            return ImmutableMap.of("timeout", "1", "timeoutUnit", "SECONDS");
          }
        };
      }
    }));

    Assert.assertEquals(Message.Type.SYSTEM, message.getType());
    Assert.assertEquals(Message.Scope.APPLICATION, message.getScope());
    Assert.assertNull(message.getRunnableName());
    Assert.assertEquals("stop", message.getCommand().getCommand());
    Assert.assertEquals(ImmutableMap.of("timeout", "1", "timeoutUnit", "SECONDS"), message.getCommand().getOptions());
  }

  @Test
  public void testFailureDecode() {
    Assert.assertNull(MessageCodec.decode("".getBytes()));
  }
}
