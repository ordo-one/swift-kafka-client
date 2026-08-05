//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2022 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Dispatch
import Logging

/// A single source delivering *all* events of one consumer — fetched messages, rebalances,
/// partition EOF and errors — so that the consumer can be driven from a single loop:
///
/// ```swift
/// let stream = try await KafkaConsumerStream(configuration: configuration, logger: logger)
/// while let event = await stream.nextEvent() {
///     switch event { ... }
/// }
/// ```
///
/// The topics (or partitions) to consume come from
/// ``KafkaConsumerConfiguration/consumptionStrategy``: the initializer subscribes for
/// `.group` and assigns for `.partitions`, so there is nothing to subscribe to by hand.
///
/// Calling ``nextEvent()`` *is* the poll loop; there is no separate service task to run.
///
/// The element is ``KafkaConsumerEvent``, *not* a message: records arrive batched inside
/// ``KafkaConsumerEvent/fetch(_:)`` (see ``KafkaFetch/withMessages(_:)``) alongside
/// rebalance, EOF and error events. This is what distinguishes the type from
/// ``KafkaConsumer``, which splits the same information across ``KafkaConsumer/messages``
/// and a separate ``KafkaConsumerEvents`` sequence and needs its `run()` method serviced.
///
/// - Important: The stream enables `.rebalance` events, which turns off librdkafka's
///   automatic partition assignment. The caller is therefore responsible for
///   (un)assigning partitions in response to every ``KafkaConsumerEvent/rebalance(_:)``
///   event, otherwise the consumer is never assigned any partitions and consumes nothing.
///
/// - Important: ``nextEvent()`` must be called from one task at a time — see its own
///   documentation.
public final class KafkaConsumerStream: @unchecked Sendable {
    /// Internal: used by `KafkaTransaction` to reach the consumer's kafka handle when
    /// committing consumed offsets transactionally (`send(offsets:forConsumer:)`).
    let client: RDKafkaClient
    private let configPollInterval: Duration
    private let metrics: KafkaConfiguration.ConsumerMetrics
    private let healthStatusEnabled: Bool
    private let executor: DispatchQueueTaskExecutor

    // Poll state: only ever touched by `nextEvent()`, which is single-consumer. This is why the
    // `Sendable` conformance is `@unchecked` — no lock guards these, the contract does.
    private var events = [RDKafkaClient.KafkaEvent]()
    private var idx = 0
    /// Current backoff, adapted on every poll; never exceeds `configPollInterval`.
    private var pollInterval: Duration

    public init(configuration: KafkaConsumerConfiguration, logger: Logger) async throws {
        // `.rebalance` makes librdkafka deliver assign/revoke events on the queue instead
        // of assigning partitions automatically; the caller must then (un)assign partitions
        // itself in response to each `.rebalance` event (see the `assign`/`incrementalAssign`
        // helpers below).
        var subscribedEvents: [RDKafkaEvent] = [.log, .rebalance]

        // Only listen to offset commit events when autoCommit is false
        if configuration.isAutoCommitEnabled == false {
            subscribedEvents.append(.offsetCommit)
        }

        if configuration.metrics.enabled || configuration.healthStatusInterval != nil {
            subscribedEvents.append(.statistics)
        }

        let client = try RDKafkaClient.makeClient(
            type: .consumer,
            configDictionary: configuration.dictionary,
            events: subscribedEvents,
            logger: logger,
            singleQueue: true
        )

        self.client = client
        self.configPollInterval = configuration.pollInterval
        self.pollInterval = configuration.pollInterval
        self.metrics = configuration.metrics
        self.healthStatusEnabled = configuration.healthStatusInterval != nil

        self.executor = DispatchQueueTaskExecutor(
            DispatchQueue(label: "com.swift-server.swift-kafka.message-consumer")
        )

        // Set up the connection from the configuration, exactly as `KafkaConsumer` does at
        // the top of its `run()`. There is no `run()` here, so this is the only such hook.
        switch configuration.consumptionStrategy._internal {
        case .group(groupID: _, topics: let topics):
            guard !topics.isEmpty else {
                break
            }
            let subscription = RDKafkaTopicPartitionList()
            for topic in topics {
                subscription.add(topic: topic, partition: KafkaPartition.unassigned)
            }
            try client.subscribe(topicPartitionList: subscription)

        case .partitions(_, let partitions):
            let assignment = RDKafkaTopicPartitionList()
            for partition in partitions {
                assignment.setOffset(topic: partition.topic, partition: partition.partition, offset: partition.offset)
            }
            try await client.assign(topicPartitionList: assignment)
        }
    }

    /// Poll for the next event, waiting until one is available.
    ///
    /// - Returns: The next ``KafkaConsumerEvent``, or `nil` once the calling task is
    ///   cancelled — so a `while let` loop ends cleanly on cancellation.
    ///
    /// - Important: Single-consumer. Calling this concurrently from more than one task
    ///   splits the event stream between the callers and is not supported.
    public func nextEvent() async -> KafkaConsumerEvent? {
        while true {
            // Honor structured-concurrency cancellation: end the sequence so callers
            // iterating in a cancelled task (or a cancelled task group) stop cleanly.
            if Task.isCancelled {
                return nil
            }
            if idx == 0 {
                let shouldSleep: Bool
                if #available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *) {
                    shouldSleep = await withTaskExecutorPreference(executor) {
                        client.eventPoll(events: &events)
                    }
                } else {
                    shouldSleep = client.eventPoll(events: &events)
                }

                if shouldSleep {
                    pollInterval = Swift.min(configPollInterval, pollInterval * 2)
                    let clock = ContinuousClock()
                    try? await clock.sleep(until: clock.now.advanced(by: pollInterval))
                } else {
                    pollInterval = Swift.max(pollInterval / 3, .microseconds(1))
                    await Task.yield()
                }

                // The poll may not have produced any events; go back and poll again.
                if events.isEmpty {
                    continue
                }
            }

            let event = events[idx]
            idx += 1

            if idx == events.count {
                events.removeAll(keepingCapacity: true)
                idx = 0
            }

            switch event {
            case let .fetch(ptr):
                return .fetch(.init(ptr))

            case let .partitionEOF(topicPartition):
                return .partitionEOF(topicPartition)

            case .deliveryReport:
                break

            case let .statistics(statistics):
                metrics.update(with: statistics)
                if healthStatusEnabled {
                    return .healthStatus(statistics.consumerHealthStatus)
                }

            case let .rebalance(action):
                return .rebalance(action)

            case let .error(error):
                return .error(error)
            }
        }
    }

    // MARK: - Rebalance handling
    //
    // The `.rebalance` event turns off librdkafka's automatic partition assignment
    // (see `rd_kafka_conf_set_rebalance_cb` in rdkafka.h). The caller is therefore
    // responsible for (un)assigning partitions in response to each `.rebalance` event
    // received from the sequence, otherwise the consumer is never assigned any
    // partitions and consumes nothing.
    //
    // For eager assignors (`range`, `roundrobin`) use ``assign(_:)`` on
    // `.assign` and ``unassignAll()`` on `.revoke`. For cooperative assignors
    // (`cooperative-sticky`) use ``incrementalAssign(_:)`` / ``incrementalUnassign(_:)``.

    /// Assign the full partition set to consume (eager assignors).
    public func assign(_ topics: KafkaTopicList) async throws {
        try await client.assign(topicPartitionList: topics.list)
    }

    /// Clear the current assignment (eager `.revoke`, or to sync state on error).
    public func unassignAll() async throws {
        try await client.assign(topicPartitionList: nil)
    }

    /// Incrementally add partitions to the current assignment (cooperative assignors).
    public func incrementalAssign(_ topics: KafkaTopicList) async throws {
        try await client.incrementalAssign(topicPartitionList: topics.list)
    }

    /// Incrementally remove partitions from the current assignment (cooperative assignors).
    public func incrementalUnassign(_ topics: KafkaTopicList) async throws {
        try await client.incrementalUnassign(topicPartitionList: topics.list)
    }
}
