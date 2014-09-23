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
package org.apache.twill.internal.json;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.internal.DefaultResourceSpecification;
import org.junit.Assert;
import org.junit.Test;
import org.unitils.reflectionassert.ReflectionAssert;

/**
 * Maybe this checkstyle rule needs to be removed.
 */
public class ResourceSpecificationCodecTest {
  private final Gson gson = new GsonBuilder().serializeNulls()
          .registerTypeAdapter(ResourceSpecification.class, new ResourceSpecificationCodec())
          .registerTypeAdapter(DefaultResourceSpecification.class, new ResourceSpecificationCodec())
          .create();

  @Test
  public void testCodec() throws Exception {
    String expectedString =
            "{" +
                    "\"cores\":2," +
                    "\"memorySize\":1024," +
                    "\"instances\":2," +
                    "\"uplink\":100," +
                    "\"downlink\":100" +
            "}";
    final ResourceSpecification expected =
            new DefaultResourceSpecification(2, 1024, 2, 100, 100);
    final String actualString = gson.toJson(expected);
    Assert.assertEquals(expectedString, actualString);

    final JsonElement expectedJson = gson.toJsonTree(expected);
    final ResourceSpecification actual = gson.fromJson(expectedJson, DefaultResourceSpecification.class);
    final JsonElement actualJson = gson.toJsonTree(actual);

    Assert.assertEquals(expectedJson, actualJson);
    ReflectionAssert.assertLenientEquals(expected, actual);
  }

  @Test
  public void testBuilder() throws Exception {
    final ResourceSpecification actual = ResourceSpecification.Builder.with()
            .setVirtualCores(5)
            .setMemory(4, ResourceSpecification.SizeUnit.GIGA)
            .setInstances(3)
            .setUplink(10, ResourceSpecification.SizeUnit.GIGA)
            .setDownlink(5, ResourceSpecification.SizeUnit.GIGA)
            .build();
    final DefaultResourceSpecification expected =
            new DefaultResourceSpecification(5, 4096, 3, 10240, 5120);
    ReflectionAssert.assertLenientEquals(expected, actual);
  }

  @Test
  public void testBuilderWithLists() throws Exception {
    final ResourceSpecification actual = ResourceSpecification.Builder.with()
            .setVirtualCores(5)
            .setMemory(4, ResourceSpecification.SizeUnit.GIGA)
            .setInstances(3)
            .setUplink(10, ResourceSpecification.SizeUnit.GIGA)
            .setDownlink(5, ResourceSpecification.SizeUnit.GIGA)
            .build();
    final DefaultResourceSpecification expected =
            new DefaultResourceSpecification(5, 4096, 3, 10240, 5120);
    ReflectionAssert.assertLenientEquals(expected, actual);
  }

}
