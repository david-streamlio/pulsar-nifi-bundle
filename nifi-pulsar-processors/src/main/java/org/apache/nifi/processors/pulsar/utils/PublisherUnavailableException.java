/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.pulsar.utils;

/**
 * Thrown by {@link PublisherPool#obtainPublisher(String)} when a topic's only producer - under an exclusive access
 * mode there is exactly one - is held by another task and did not come back within the pool's wait, or the wait
 * was interrupted. Nothing was attempted for the caller's FlowFile: it should go back to its queue for a later
 * trigger, not to {@code failure}.
 */
public class PublisherUnavailableException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public PublisherUnavailableException(final String message) {
        super(message);
    }

    public PublisherUnavailableException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
