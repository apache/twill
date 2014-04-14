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

import com.google.common.collect.ImmutableSet;
import com.google.common.io.InputSupplier;
import com.google.common.io.OutputSupplier;
import org.apache.twill.internal.JvmOptions;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Collections;
import java.util.List;

/**
 * Tests the JvmOptions Codec.
 */
public class JvmOptionsCodecTest {

  @Test
  public void testNoNulls() throws Exception {
    JvmOptions options = new JvmOptions("-version",
                                        new JvmOptions.DebugOptions(true, false, ImmutableSet.of("one", "two")));
    final StringWriter writer = new StringWriter();
    JvmOptionsCodec.encode(options, new OutputSupplier<Writer>() {
      @Override
      public Writer getOutput() throws IOException {
        return writer;
      }
    });
    JvmOptions options1 = JvmOptionsCodec.decode(new InputSupplier<Reader>() {
      @Override
      public Reader getInput() throws IOException {
        return new StringReader(writer.toString());
      }
    });
    Assert.assertEquals(options.getExtraOptions(), options1.getExtraOptions());
    Assert.assertEquals(options.getDebugOptions().doDebug(), options1.getDebugOptions().doDebug());
    Assert.assertEquals(options.getDebugOptions().doSuspend(), options1.getDebugOptions().doSuspend());
    Assert.assertEquals(options.getDebugOptions().getRunnables(), options1.getDebugOptions().getRunnables());
  }

  @Test
  public void testSomeNulls() throws Exception {
    JvmOptions options = new JvmOptions(null, new JvmOptions.DebugOptions(false, false, null));
    final StringWriter writer = new StringWriter();
    JvmOptionsCodec.encode(options, new OutputSupplier<Writer>() {
      @Override
      public Writer getOutput() throws IOException {
        return writer;
      }
    });
    JvmOptions options1 = JvmOptionsCodec.decode(new InputSupplier<Reader>() {
      @Override
      public Reader getInput() throws IOException {
        return new StringReader(writer.toString());
      }
    });
    Assert.assertEquals(options.getExtraOptions(), options1.getExtraOptions());
    Assert.assertEquals(options.getDebugOptions().doDebug(), options1.getDebugOptions().doDebug());
    Assert.assertEquals(options.getDebugOptions().doSuspend(), options1.getDebugOptions().doSuspend());
    Assert.assertEquals(options.getDebugOptions().getRunnables(), options1.getDebugOptions().getRunnables());
  }

  @Test
  public void testNoRunnables() throws Exception {
    List<String> noRunnables = Collections.emptyList();
    JvmOptions options = new JvmOptions(null, new JvmOptions.DebugOptions(true, false, noRunnables));
    final StringWriter writer = new StringWriter();
    JvmOptionsCodec.encode(options, new OutputSupplier<Writer>() {
      @Override
      public Writer getOutput() throws IOException {
        return writer;
      }
    });
    JvmOptions options1 = JvmOptionsCodec.decode(new InputSupplier<Reader>() {
      @Override
      public Reader getInput() throws IOException {
        return new StringReader(writer.toString());
      }
    });
    Assert.assertEquals(options.getExtraOptions(), options1.getExtraOptions());
    Assert.assertEquals(options.getDebugOptions().doDebug(), options1.getDebugOptions().doDebug());
    Assert.assertEquals(options.getDebugOptions().doSuspend(), options1.getDebugOptions().doSuspend());
    Assert.assertEquals(options.getDebugOptions().getRunnables(), options1.getDebugOptions().getRunnables());
  }
}
