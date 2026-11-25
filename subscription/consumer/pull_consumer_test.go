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

	consumer.observeHeartbeatEndpointsLocked(map[int32]string{
		1: "dn1:6667",
		2: "dn2:6668",
		3: "dn3:6669",
		4: "dn4:6667",
	})
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

func TestRetainedSnapshotTrimsWhitespaceInTopicAttributes(t *testing.T) {
	consumer := newTerminationTestConsumer()
	consumer.topics["snapshot_ws"] = map[string]string{
		"mode":                              " snapshot ",
		"retain.progress.after.unsubscribe": " true ",
	}
	consumer.stagePendingTerminations([]subscriptionTermination{
		{topic: "snapshot_ws", dataNodeID: 1},
	})
	assertPending(t, consumer, "snapshot_ws", false)

	consumer.stagePendingTerminations([]subscriptionTermination{
		{topic: "snapshot_ws", dataNodeID: 2},
		{topic: "snapshot_ws", dataNodeID: 3},
	})
	assertPending(t, consumer, "snapshot_ws", true)
}

func TestRetainedSnapshotRejectsStalePollTerminationAfterHeartbeatOrTopologyChange(t *testing.T) {
	consumer := newTerminationTestConsumer()
	provider1 := &subscriptionProvider{dataNodeID: 1, endpoint: "dn1:6667", epoch: 1}
	consumer.providers[1] = provider1

	staleFromPoll := subscriptionTermination{
		topic:                "snapshot",
		dataNodeID:           1,
		provider:             provider1,
		providerEpoch:        provider1.epoch,
		topologyGeneration:   consumer.topologyGeneration,
		completionGeneration: consumer.completionGeneration,
	}

	// Simulate another node's completion arriving and then DN1's completion being revoked
	// before the stale poll termination is staged.
	consumer.stagePendingTerminations([]subscriptionTermination{
		{topic: "snapshot", dataNodeID: 1},
		{topic: "snapshot", dataNodeID: 2},
	})
	consumer.invalidateProviderTopicCompletionsLocked(1, nil)

	consumer.stagePendingTerminations([]subscriptionTermination{staleFromPoll})
	if _, exists := consumer.terminatedDataNodeIDsByTopic["snapshot"][1]; exists {
		t.Fatal("stale poll termination was not rejected after completionGeneration advanced")
	}

	// Also reject after topologyGeneration advances.
	staleTopo := subscriptionTermination{
		topic:                "snapshot",
		dataNodeID:           1,
		provider:             provider1,
		providerEpoch:        provider1.epoch,
		topologyGeneration:   consumer.topologyGeneration,
		completionGeneration: consumer.completionGeneration,
	}
	consumer.observeHeartbeatEndpointsLocked(map[int32]string{
		1: "dn1:6667",
		2: "dn2:6668",
		3: "dn3:6669",
		4: "dn4:6667",
	})
	consumer.stagePendingTerminations([]subscriptionTermination{staleTopo})
	if _, exists := consumer.terminatedDataNodeIDsByTopic["snapshot"][1]; exists {
		t.Fatal("stale poll termination was not rejected after topologyGeneration advanced")
	}
}

func TestRetainedSnapshotInvalidatesOnProviderReconnectWithSameAddress(t *testing.T) {
	consumer := newTerminationTestConsumer()
	oldProvider := &subscriptionProvider{dataNodeID: 1, endpoint: "dn1:6667", epoch: 1}
	consumer.providers[1] = oldProvider
	consumer.stagePendingTerminations([]subscriptionTermination{
		{topic: "snapshot", dataNodeID: 1},
		{topic: "snapshot", dataNodeID: 2},
	})
	if len(consumer.terminatedDataNodeIDsByTopic["snapshot"]) != 2 {
		t.Fatalf("expected 2 terminated nodes before reconnect")
	}

	newProvider := &subscriptionProvider{dataNodeID: 1, endpoint: "dn1:6667"}
	consumer.mu.Lock()
	previous := consumer.providers[newProvider.dataNodeID]
	newProvider.epoch = consumer.nextProviderEpochLocked()
	consumer.providers[newProvider.dataNodeID] = newProvider
	if previous != nil && previous != newProvider {
		consumer.topologyGeneration++
		consumer.clearTerminatedProviderStatesLocked()
	}
	consumer.mu.Unlock()

	if len(consumer.terminatedDataNodeIDsByTopic) != 0 {
		t.Fatal("provider reconnect with same address did not clear cached completions")
	}
}

func TestRetainedSnapshotHeartbeatShrinkInvalidatesWithoutDroppingEndpoints(t *testing.T) {
	consumer := newTerminationTestConsumer()
	consumer.stagePendingTerminations([]subscriptionTermination{
		{topic: "snapshot", dataNodeID: 1},
		{topic: "snapshot", dataNodeID: 3},
	})

	// Node 2 advertises only {1, 2}, omitting known node 3.
	shrank := consumer.observeHeartbeatEndpointsLocked(map[int32]string{
		1: "dn1:6667",
		2: "dn2:6668",
	})
	if !shrank {
		t.Fatal("expected shrank=true when advertised endpoints omit known node 3")
	}
	if len(consumer.terminatedDataNodeIDsByTopic) != 0 {
		t.Fatal("shrinking heartbeat did not invalidate cached completions")
	}
	if _, stillKnown := consumer.knownClusterDataNodeIDs[3]; !stillKnown {
		t.Fatal("heartbeat shrink blindly dropped node 3 before full recovery")
	}
}

func TestRetainedSnapshotUnprovenDiscoveryDoesNotAutoUnsubscribe(t *testing.T) {
	consumer := newTerminationTestConsumer()
	consumer.endpointsDiscovered = false
	clear(consumer.knownClusterDataNodeIDs)
	consumer.endpoints = map[int32]string{1: "dn1:6667"}

	consumer.stagePendingTerminations([]subscriptionTermination{
		{topic: "snapshot", dataNodeID: 1},
	})
	assertPending(t, consumer, "snapshot", false)
}

func TestRetainedSnapshotBarrierFilterDropsRevokedPendingUnsubscribe(t *testing.T) {
	consumer := newTerminationTestConsumer()
	consumer.stagePendingTerminations([]subscriptionTermination{
		{topic: "snapshot", dataNodeID: 1},
		{topic: "snapshot", dataNodeID: 2},
		{topic: "snapshot", dataNodeID: 3},
	})
	consumer.mu.Lock()
	topics, barriers := consumer.eligiblePendingTerminationsLocked()
	consumer.mu.Unlock()
	if len(topics) != 1 || topics[0] != "snapshot" {
		t.Fatalf("expected snapshot eligible before revocation, got %v", topics)
	}

	// Heartbeat revokes DN 2 before unsubscribe RPC is dispatched.
	consumer.mu.Lock()
	consumer.invalidateProviderTopicCompletionsLocked(2, nil)
	filtered := consumer.filterAutomaticUnsubscribesLocked(topics, barriers)
	consumer.mu.Unlock()
	if len(filtered) != 0 {
		t.Fatalf("expected revoked snapshot to be filtered out, got %v", filtered)
	}
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
		knownClusterDataNodeIDs: map[int32]struct{}{
			1: {},
			2: {},
			3: {},
		},
		endpointsDiscovered:          true,
		providers:                    make(map[int32]*subscriptionProvider),
		pendingTerminations:          make(map[string]struct{}),
		pendingUnsubscribes:          make(map[string]struct{}),
		pendingTerminationBarrier:    make(map[string]subscriptionTerminationBarrier),
		terminatedDataNodeIDsByTopic: make(map[string]map[int32]struct{}),
	}
}

func assertPending(t *testing.T, consumer *subscriptionPullConsumer, topic string, want bool) {
	t.Helper()
	if _, exists := consumer.pendingTerminations[topic]; exists != want {
		t.Fatalf("pending termination for %q = %t, want %t", topic, exists, want)
	}
}
