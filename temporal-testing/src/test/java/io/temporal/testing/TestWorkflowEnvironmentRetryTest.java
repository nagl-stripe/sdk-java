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

package io.temporal.testing;

import io.temporal.activity.Activity;
import io.temporal.activity.ActivityInfo;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import io.temporal.activity.ActivityOptions;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.common.RetryOptions;
import io.temporal.worker.Worker;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class TestWorkflowEnvironmentRetryTest {

  private static final int BACKOFF_COEFFICIENT = 2;
  public static final RetryOptions RETRY_POLICY =
      RetryOptions.newBuilder()
          .setInitialInterval(Duration.ofSeconds(10))
          .setBackoffCoefficient(BACKOFF_COEFFICIENT)
          .build();

  @Rule
  public TestWatcher watchman =
      new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
          if (testEnv != null) {
            System.err.println(testEnv.getDiagnostics());
            testEnv.close();
          }
        }
      };

  private RetryingActivityImpl retryingActivity;

  @WorkflowInterface
  public interface ExampleWorkflow {
    @WorkflowMethod
    void execute();
  }

  public static class RetryingWorkflow implements ExampleWorkflow {
    @Override
    public void execute() {
      Workflow.newActivityStub(
              RetryingActivity.class,
              ActivityOptions.newBuilder()
                  .setStartToCloseTimeout(Duration.ofDays(365))
                  .setRetryOptions(RETRY_POLICY)
                  .build())
          .maybeRetry();
    }
  }

  @ActivityInterface
  public interface RetryingActivity {
    @ActivityMethod
    public void maybeRetry();
  }

  public class RetryingActivityImpl implements RetryingActivity {

    public List<Instant> attemptTimes = new ArrayList<>();

    @Override
    public void maybeRetry() {
      ActivityInfo info = Activity.getExecutionContext().getInfo();
      Instant now = Instant.ofEpochMilli(info.getCurrentAttemptScheduledTimestamp());
      attemptTimes.add(now);
      if (info.getAttempt() < 5) {
        throw new RuntimeException(
            String.format("induce a retry on attempt %d at %s", info.getAttempt(), now));
      }
    }
  }

  private TestWorkflowEnvironment testEnv;
  private Worker worker;
  private WorkflowClient client;
  private static final String WORKFLOW_TASK_QUEUE = "EXAMPLE";

  @Before
  public void setUp() {
    setUp(TestEnvironmentOptions.getDefaultInstance());
  }

  private void setUp(TestEnvironmentOptions options) {
    testEnv = TestWorkflowEnvironment.newInstance(options);
    worker = testEnv.newWorker(WORKFLOW_TASK_QUEUE);
    client = testEnv.getWorkflowClient();
    worker.registerWorkflowImplementationTypes(RetryingWorkflow.class);
    retryingActivity = new RetryingActivityImpl();
    worker.registerActivitiesImplementations(retryingActivity);
    testEnv.start();
  }

  @After
  public void tearDown() {
    testEnv.close();
  }

  @Test
  public void testAttemptScheduling() {
    ExampleWorkflow workflow =
        client.newWorkflowStub(
            ExampleWorkflow.class,
            WorkflowOptions.newBuilder().setTaskQueue(WORKFLOW_TASK_QUEUE).build());

    workflow.execute();

    List<Duration> timeBetweenAttempts = new ArrayList<>();
    Instant previousAttempt = null;
    for (Instant attempt : retryingActivity.attemptTimes) {
      if (previousAttempt != null) {
        timeBetweenAttempts.add(Duration.between(previousAttempt, attempt));
      }

      previousAttempt = attempt;
    }

    List<Duration> expectedTimeBetweenAttempts = new ArrayList<>();
    for (int i = 0; i < timeBetweenAttempts.size(); i++) {
      if (expectedTimeBetweenAttempts.isEmpty()) {
        expectedTimeBetweenAttempts.add(RETRY_POLICY.getInitialInterval());
      } else {
        Duration last = expectedTimeBetweenAttempts.get(expectedTimeBetweenAttempts.size() - 1);
        expectedTimeBetweenAttempts.add(last.multipliedBy(BACKOFF_COEFFICIENT));
      }
    }

    for (int i = 0; i < expectedTimeBetweenAttempts.size(); i++) {
      // Because real time still passes on the test server's clock, we need to allow for some
      // variance. Let's do 5% either way.
      Duration expected = expectedTimeBetweenAttempts.get(i);
      Duration lowerBound = expected.minusMillis(expected.toMillis() / 20);
      Duration upperBound = expected.plusMillis(expected.toMillis() / 20);

      Duration actual = timeBetweenAttempts.get(i);
      String message =
          String.format("%s should be between %s and %s", actual, lowerBound, upperBound);
      Assert.assertTrue(
          message, actual.compareTo(lowerBound) >= 0 && actual.compareTo(upperBound) <= 0);
    }
  }
}
