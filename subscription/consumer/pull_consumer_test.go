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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package consumer

import "testing"

func TestAllConsumedWaitsForSubscriptionRemoval(t *testing.T) {
	consumer := &subscriptionPullConsumer{
		topics:              map[string]map[string]string{"topic": {}},
		pendingTerminations: map[string]struct{}{"topic": {}},
	}

	if consumer.AllConsumed() {
		t.Fatal("pending termination reported completion while topic was still subscribed")
	}

	delete(consumer.topics, "topic")
	if !consumer.AllConsumed() {
		t.Fatal("empty subscription set did not report completion")
	}
}

func TestRetainedSnapshotWaitsForAllExpectedDataNodes(t *testing.T) {
	consumer := newTerminationTestConsumer()
	consumer.stagePendingTerminations([]subscriptionTermination{
		{topic: "snapshot", dataNodeID: 1},
		{topic: "inc", dataNodeID: 1},
	})
	assertPending(t, consumer, "snapshot", false)
	assertPending(t, consumer, "inc", true)

	consumer.stagePendingTerminations([]subscriptionTermination{
		{topic: "snapshot", dataNodeID: 2},
	})
	assertPending(t, consumer, "snapshot", false)

	consumer.stagePendingTerminations([]subscriptionTermination{
		{topic: "snapshot", dataNodeID: 3},
	})
	assertPending(t, consumer, "snapshot", true)
}

func TestRetainedSnapshotCompletionIsRevokedByHeartbeat(t *testing.T) {
	consumer := newTerminationTestConsumer()
	for dataNodeID := int32(1); dataNodeID <= 3; dataNodeID++ {
		consumer.stagePendingTerminations([]subscriptionTermination{
			{topic: "snapshot", dataNodeID: dataNodeID},
		})
	}
	assertPending(t, consumer, "snapshot", true)

	consumer.invalidateProviderTopicCompletionsLocked(2, nil)
	assertPending(t, consumer, "snapshot", false)
	if _, exists := consumer.terminatedDataNodeIDsByTopic["snapshot"][2]; exists {
		t.Fatal("heartbeat did not revoke the DataNode completion")
	}
}

func TestRetainedSnapshotCompletionIsClearedByTopologyChange(t *testing.T) {
	consumer := newTerminationTestConsumer()
	for dataNodeID := int32(1); dataNodeID <= 3; dataNodeID++ {
		consumer.stagePendingTerminations([]subscriptionTermination{
			{topic: "snapshot", dataNodeID: dataNodeID},
		})
	}
	assertPending(t, consumer, "snapshot", true)

	consumer.observeEndpointLocked(4, "dn4:6667")
	assertPending(t, consumer, "snapshot", false)
	if consumer.topologyGeneration == 0 {
		t.Fatal("topology change did not advance the generation")
	}
}

func TestRetainedSnapshotCompletionRequiresTopicToRemainSubscribed(t *testing.T) {
	consumer := newTerminationTestConsumer()
	consumer.stagePendingTerminations([]subscriptionTermination{
		{topic: "snapshot", dataNodeID: 1},
		{topic: "snapshot", dataNodeID: 2},
		{topic: "snapshot", dataNodeID: 3},
	})
	assertPending(t, consumer, "snapshot", true)

	delete(consumer.topics, "snapshot")
	consumer.invalidateProviderTopicCompletionsLocked(1, nil)
	consumer.stagePendingTerminations([]subscriptionTermination{
		{topic: "snapshot", dataNodeID: 1},
	})
	assertPending(t, consumer, "snapshot", false)
}

func newTerminationTestConsumer() *subscriptionPullConsumer {
	return &subscriptionPullConsumer{
		topics: map[string]map[string]string{
			"snapshot": {
				"mode":                              "snapshot",
				"retain.progress.after.unsubscribe": "true",
			},
			"inc": {},
		},
		endpoints: map[int32]string{
			1: "dn1:6667",
			2: "dn2:6668",
			3: "dn3:6669",
		},
		providers:                    make(map[int32]*subscriptionProvider),
		pendingTerminations:          make(map[string]struct{}),
		pendingUnsubscribes:          make(map[string]struct{}),
		terminatedDataNodeIDsByTopic: make(map[string]map[int32]struct{}),
	}
}

func assertPending(t *testing.T, consumer *subscriptionPullConsumer, topic string, want bool) {
	t.Helper()
	if _, exists := consumer.pendingTerminations[topic]; exists != want {
		t.Fatalf("pending termination for %q = %t, want %t", topic, exists, want)
	}
}
