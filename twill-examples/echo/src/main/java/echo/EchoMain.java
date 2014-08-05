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
package echo;

import com.google.common.base.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Simple application used for BundledJarExample and RuntimeBundledJarExample.
 */
public class EchoMain {
  private static final Logger logger = LoggerFactory.getLogger(EchoMain.class);

  public static void main(String[] args) {
    logger.info("Hello from EchoMain: " + new TestConverter().convert("sdflkj"));
    System.err.println("err HELLO from scatch");
    System.out.println("out HELLO from scatch");
    logger.info("Got args: " + Arrays.toString(args));
    System.out.println("Got args: " + Arrays.toString(args));
  }

  private static final class TestConverter extends Converter<String, Integer> {

    @Override
    protected Integer doForward(String s) {
      return s.length();
    }

    @Override
    protected String doBackward(Integer integer) {
      return integer.toString();
    }
  }

}
