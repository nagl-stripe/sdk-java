/*
 *  Copyright (C) 2020 Temporal Technologies, Inc. All Rights Reserved.
 *
 *  Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Modifications copyright (C) 2017 Uber Technologies, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"). You may not
 *  use this file except in compliance with the License. A copy of the License is
 *  located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package io.temporal.internal.testservice;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Optional;

class CronUtils {

  static Cron parseCron(String schedule) {
    CronDefinition cronDefinition = CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX);
    CronParser parser = new CronParser(cronDefinition);
    return parser.parse(schedule);
  }

  static Duration getBackoffInterval(String schedule, Timestamp timestamp) {
    if (schedule == null || schedule.isEmpty()) {
      return Duration.ZERO;
    }
    Cron cron = parseCron(schedule);

    Instant i = Instant.ofEpochMilli(Timestamps.toMillis(timestamp));
    ZonedDateTime now = ZonedDateTime.ofInstant(i, ZoneOffset.UTC);

    ExecutionTime executionTime = ExecutionTime.forCron(cron);
    Optional<Duration> backoff = executionTime.timeToNextExecution(now);
    Duration backoffInterval = backoff.orElse(Duration.ZERO);

    if (backoffInterval == Duration.ZERO) {
      backoff = executionTime.timeToNextExecution(now.plusSeconds(1));
      backoffInterval = backoff.get();
    }

    return backoffInterval;
  }
}
