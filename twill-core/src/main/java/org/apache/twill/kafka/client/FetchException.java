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
package org.apache.twill.kafka.client;

/**
 *
 */
public final class FetchException extends RuntimeException {

  private final ErrorCode errorCode;

  public FetchException(String message, ErrorCode errorCode) {
    super(message);
    this.errorCode = errorCode;
  }

  public ErrorCode getErrorCode() {
    return errorCode;
  }

  @Override
  public String toString() {
    return String.format("%s. Error code: %s", super.toString(), errorCode);
  }

  public enum ErrorCode {
    UNKNOWN(-1),
    OK(0),
    OFFSET_OUT_OF_RANGE(1),
    INVALID_MESSAGE(2),
    WRONG_PARTITION(3),
    INVALID_FETCH_SIZE(4);

    private final int code;

    ErrorCode(int code) {
      this.code = code;
    }

    public int getCode() {
      return code;
    }

    public static ErrorCode fromCode(int code) {
      switch (code) {
        case -1:
          return UNKNOWN;
        case 0:
          return OK;
        case 1:
          return OFFSET_OUT_OF_RANGE;
        case 2:
          return INVALID_MESSAGE;
        case 3:
          return WRONG_PARTITION;
        case 4:
          return INVALID_FETCH_SIZE;
      }
      throw new IllegalArgumentException("Unknown error code");
    }
  }
}
