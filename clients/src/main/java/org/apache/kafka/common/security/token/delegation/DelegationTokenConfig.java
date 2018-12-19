/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.security.token.delegation;

import java.util.Objects;

public final class DelegationTokenConfig {
    private final long delegationTokenMaxLifeMs;
    private final long delegationTokenExpiryTimeMs;
    private final long delegationTokenExpiryCheckIntervalMs;
    private final boolean tokenAuthEnabled;

    public DelegationTokenConfig(long delegationTokenMaxLifeMs, long delegationTokenExpiryTimeMs, long delegationTokenExpiryCheckIntervalMs,
                                 boolean tokenAuthEnabled) {
        this.delegationTokenMaxLifeMs = delegationTokenMaxLifeMs;
        this.delegationTokenExpiryTimeMs = delegationTokenExpiryTimeMs;
        this.delegationTokenExpiryCheckIntervalMs = delegationTokenExpiryCheckIntervalMs;
        this.tokenAuthEnabled = tokenAuthEnabled;
    }

    public boolean tokenAuthEnabled() {
        return tokenAuthEnabled;
    }

    public long delegationTokenMaxLifeMs() {
        return delegationTokenMaxLifeMs;
    }

    public long delegationTokenExpiryTimeMs() {
        return delegationTokenExpiryTimeMs;
    }

    public long delegationTokenExpiryCheckIntervalMs() {
        return delegationTokenExpiryCheckIntervalMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DelegationTokenConfig that = (DelegationTokenConfig) o;
        return delegationTokenMaxLifeMs == that.delegationTokenMaxLifeMs &&
               delegationTokenExpiryTimeMs == that.delegationTokenExpiryTimeMs &&
               delegationTokenExpiryCheckIntervalMs == that.delegationTokenExpiryCheckIntervalMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(delegationTokenMaxLifeMs, delegationTokenExpiryTimeMs, delegationTokenExpiryCheckIntervalMs);
    }

    @Override
    public String toString() {
        return "DelegationTokenConfig{" +
               "delegationTokenMaxLifeMs=" + delegationTokenMaxLifeMs +
               ", delegationTokenExpiryTimeMs=" + delegationTokenExpiryTimeMs +
               ", delegationTokenExpiryCheckIntervalMs=" + delegationTokenExpiryCheckIntervalMs +
               '}';
    }
}
