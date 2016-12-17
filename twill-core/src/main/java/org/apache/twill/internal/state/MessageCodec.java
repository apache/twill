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

import com.google.common.base.Charsets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.apache.twill.api.Command;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Gson codec for {@link Message}.
 */
public final class MessageCodec {

  private static final Type OPTIONS_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Gson GSON = new GsonBuilder()
                                        .serializeNulls()
                                        .registerTypeAdapter(Message.class, new MessageAdapter())
                                        .registerTypeAdapter(Command.class, new CommandAdapter())
                                        .create();

  /**
   * Decodes a {@link Message} from the given byte array.
   * @param bytes byte array to be decoded
   * @return Message decoded or {@code null} if fails to decode.
   */
  public static Message decode(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    String content = new String(bytes, Charsets.UTF_8);
    return GSON.fromJson(content, Message.class);
  }

  /**
   * Encodes a {@link Message} into byte array. Revserse of {@link #decode(byte[])} method.
   * @param message Message to be encoded
   * @return byte array representing the encoded message.
   */
  public static byte[] encode(Message message) {
    return GSON.toJson(message, Message.class).getBytes(Charsets.UTF_8);
  }

  /**
   * Gson codec for {@link Message} object.
   */
  private static final class MessageAdapter implements JsonSerializer<Message>, JsonDeserializer<Message> {

    @Override
    public Message deserialize(JsonElement json, Type typeOfT,
                               JsonDeserializationContext context) throws JsonParseException {
      JsonObject jsonObj = json.getAsJsonObject();

      Message.Type type = Message.Type.valueOf(jsonObj.get("type").getAsString());
      Message.Scope scope = Message.Scope.valueOf(jsonObj.get("scope").getAsString());
      JsonElement name = jsonObj.get("runnableName");
      String runnableName = (name == null || name.isJsonNull()) ? null : name.getAsString();
      Command command = context.deserialize(jsonObj.get("command"), Command.class);

      return new SimpleMessage(type, scope, runnableName, command);
    }

    @Override
    public JsonElement serialize(Message message, Type typeOfSrc, JsonSerializationContext context) {
      JsonObject jsonObj = new JsonObject();
      jsonObj.addProperty("type", message.getType().name());
      jsonObj.addProperty("scope", message.getScope().name());
      jsonObj.addProperty("runnableName", message.getRunnableName());
      jsonObj.add("command", context.serialize(message.getCommand(), Command.class));

      return jsonObj;
    }
  }

  /**
   * Gson codec for {@link Command} object.
   */
  private static final class CommandAdapter implements JsonSerializer<Command>, JsonDeserializer<Command> {

    @Override
    public Command deserialize(JsonElement json, Type typeOfT,
                               JsonDeserializationContext context) throws JsonParseException {
      JsonObject jsonObj = json.getAsJsonObject();
      return Command.Builder.of(jsonObj.get("command").getAsString())
                            .addOptions(context.<Map<String, String>>deserialize(jsonObj.get("options"), OPTIONS_TYPE))
                            .build();
    }

    @Override
    public JsonElement serialize(Command command, Type typeOfSrc, JsonSerializationContext context) {
      JsonObject jsonObj = new JsonObject();
      jsonObj.addProperty("command", command.getCommand());
      jsonObj.add("options", context.serialize(command.getOptions(), OPTIONS_TYPE));
      return jsonObj;
    }
  }

  private MessageCodec() {
  }
}
