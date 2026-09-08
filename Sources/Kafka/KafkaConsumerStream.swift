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
/// try await stream.close()
/// ```
///
/// The topics (or partitions) to consume come from
/// ``KafkaConsumerConfiguration/consumptionStrategy``: the initializer subscribes for
/// `.group` and assigns for `.partitions`, so there is nothing to subscribe to by hand.
///
/// Calling ``nextEvent()`` *is* the poll loop; there is no separate service task to run.
///
/// The element is ``KafkaConsumerEvent``, *not* a message: records arrive batched inside
/// ``KafkaConsumerEvent/fetch(_:)``, a `Sequence` of ``KafkaConsumerStream/Message``, alongside
/// rebalance, EOF and error events. A message keeps its fetch event alive, so it can be collected
/// and read after the event has been handled. This is what distinguishes the type from
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
///
/// - Important: Call ``close()`` when done to commit the outstanding offsets and leave the
///   consumer group gracefully. It shares ``nextEvent()``'s single-task contract, so call it once
///   the event loop has returned rather than from another task.
///
/// - Warning: Release every ``KafkaConsumerStream/Message`` (and every ``KafkaFetch`` they came
///   from) *before* the `KafkaConsumerStream` itself is released. A fetch event that outlives the
///   stream **deadlocks the process**: the stream's deinitializer runs `rd_kafka_destroy`, which
///   waits for librdkafka's broker threads to terminate, and those threads cannot terminate while
///   an undestroyed fetch event still references them. ``KafkaFetch`` does not retain the client,
///   so nothing enforces this ordering — in practice, keep the messages in a narrower scope than
///   the stream, or clear the collection holding them before the stream goes away.
public final class KafkaConsumerStream: @unchecked Sendable {
    private let client: RDKafkaClient
    private let configPollInterval: Duration
    private let metrics: KafkaConfiguration.ConsumerMetrics
    private let healthStatusEnabled: Bool
    private let executor: DispatchQueueTaskExecutor
    private let logger: Logger

    // Poll state: only ever touched by `nextEvent()` and `close()`, which are single-consumer.
    // This is why the `Sendable` conformance is `@unchecked` — no lock guards these, the
    // contract does.
    private var events = [RDKafkaClient.KafkaEvent]()
    private var idx = 0
    /// Current backoff, adapted on every poll; never exceeds `configPollInterval`.
    private var pollInterval: Duration
    /// Set by ``close()``: makes ``nextEvent()`` return `nil` and the (un)assign methods throw.
    private var isClosed = false

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
        self.logger = logger

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

    deinit {
        if !isClosed {
            logger.warning(
                """
                KafkaConsumerStream deinitialized without close(): the consumer group was left \
                without a graceful close
                """
            )
        }
        // Release order between stored properties is unspecified, and `client`'s deinit runs
        // `rd_kafka_destroy`, which waits for broker threads that a buffered fetch event still
        // holds references to. A deinit body runs before the properties are released, so
        // dropping the batch here is what keeps that ordering from deadlocking.
        events.removeAll()
    }

    /// Poll for the next event, waiting until one is available.
    ///
    /// - Returns: The next ``KafkaConsumerEvent``, or `nil` once the calling task is
    ///   cancelled or the stream has been closed — so a `while let` loop ends cleanly on
    ///   cancellation.
    ///
    /// - Important: Single-consumer. Calling this concurrently from more than one task
    ///   splits the event stream between the callers and is not supported.
    public func nextEvent() async -> KafkaConsumerEvent? {
        while true {
            // A closed consumer has nothing left to deliver. Also honor structured-concurrency
            // cancellation: end the sequence so callers iterating in a cancelled task (or a
            // cancelled task group) stop cleanly.
            if isClosed || Task.isCancelled {
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
            case let .fetch(fetch):
                return .fetch(fetch)

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

    // MARK: - Closing

    /// Gracefully close the underlying Kafka consumer.
    ///
    /// Commits the outstanding offsets, revokes the assignment and leaves the consumer group,
    /// returning only once `librdkafka` reports the close as complete. Idempotent, and safe to
    /// call from a task that has already been cancelled — the close handshake itself is not
    /// cancellable.
    ///
    /// Afterwards ``nextEvent()`` returns `nil` and the (un)assign methods throw. The
    /// `librdkafka` handle and its event queue are released once the `KafkaConsumerStream`
    /// itself is deinitialized.
    ///
    /// - Parameter timeout: How long to wait for the close handshake before giving up.
    /// - Throws: A ``KafkaError`` if the close could not be initiated, or if it did not
    ///   complete within `timeout`.
    ///
    /// - Important: Must not overlap a ``nextEvent()`` call — invoke it once the event loop has
    ///   returned, from the task that drove that loop.
    public func close(timeout: Duration = .seconds(30)) async throws {
        guard !isClosed else {
            return
        }
        // Set before anything can fail so that a second call is a no-op either way.
        isClosed = true

        // Events that were polled but never handed out by `nextEvent()`.
        discardPendingEvents()

        try client.consumerClose()

        // `consumerClose()` only *initiates* the close: the queue has to be served until
        // `librdkafka` reports the consumer closed, otherwise the offset commit and the group
        // leave never complete. The poll blocks, so it runs on `executor`'s queue rather than on
        // the cooperative thread pool — and `performBlockingCall` is continuation-based, so
        // unlike `Clock.sleep` it is unaffected by cancellation. That matters because `close()`
        // is typically called right after cancellation ended the event loop.
        let clock = ContinuousClock()
        let deadline = clock.now.advanced(by: timeout)

        while !client.isConsumerClosed {
            let remaining = clock.now.duration(to: deadline)
            guard remaining > .zero else {
                throw KafkaError.connectionClosed(
                    reason: "Timed out after \(timeout) waiting for the Kafka consumer to close"
                )
            }
            let blockFor = Swift.min(remaining, configPollInterval)
            await performBlockingCall(queue: executor.queue) {
                _ = self.client.eventPoll(events: &self.events, blockingFor: blockFor)
            }
            await handleEventsWhileClosing()
        }
    }

    /// Serve the events that arrive while the consumer is closing.
    ///
    /// Everything is discarded — the caller's event loop is over by now — except the final
    /// revoke, which has to be answered for the close to make progress.
    private func handleEventsWhileClosing() async {
        for event in events {
            switch event {
            case .fetch:
                // Records that were already in flight when the close began; releasing the
                // batch below frees them.
                break

            case let .rebalance(action):
                // The stream enables `.rebalance` events, which turns off librdkafka's automatic
                // partition assignment — so the revoke that closing triggers has to be answered
                // here, otherwise the consumer never reaches the closed state.
                do {
                    switch action {
                    case let .revoke(rebalanceProtocol, topics) where rebalanceProtocol == .cooperative:
                        try await client.incrementalUnassign(topicPartitionList: topics.list)
                    case .revoke, .assign, .error:
                        try await client.assign(topicPartitionList: nil)
                    }
                } catch {
                    logger.debug("Unassigning partitions while closing failed: \(error)")
                }

            case let .error(error):
                logger.debug("Error received while closing: \(error)")

            case .statistics, .partitionEOF, .deliveryReport:
                break
            }
        }
        events.removeAll(keepingCapacity: true)
        idx = 0
    }

    /// Release the events that ``nextEvent()`` polled but has not delivered yet.
    ///
    /// `eventPoll` hands `.fetch` events over to ``KafkaFetch``, which destroys them in its
    /// `deinit`, so dropping the buffered events releases the ones never delivered.
    private func discardPendingEvents() {
        events.removeAll(keepingCapacity: true)
        idx = 0
    }

    /// Guards the methods that reach into `librdkafka` on the caller's behalf.
    private func ensureNotClosed() throws {
        guard !isClosed else {
            throw KafkaError.connectionClosed(reason: "KafkaConsumerStream is closed")
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
        try ensureNotClosed()
        try await client.assign(topicPartitionList: topics.list)
    }

    /// Clear the current assignment (eager `.revoke`, or to sync state on error).
    public func unassignAll() async throws {
        try ensureNotClosed()
        try await client.assign(topicPartitionList: nil)
    }

    /// Incrementally add partitions to the current assignment (cooperative assignors).
    public func incrementalAssign(_ topics: KafkaTopicList) async throws {
        try ensureNotClosed()
        try await client.incrementalAssign(topicPartitionList: topics.list)
    }

    /// Incrementally remove partitions from the current assignment (cooperative assignors).
    public func incrementalUnassign(_ topics: KafkaTopicList) async throws {
        try ensureNotClosed()
        try await client.incrementalUnassign(topicPartitionList: topics.list)
    }

    /// Move the fetch position of the given partitions, which must be currently assigned.
    ///
    /// Each entry's offset is the next offset to fetch. Waits for the seek to be applied unless
    /// `timeout` is `.zero`, in which case it is only queued.
    ///
    /// - Note: Seeking from a `.revoke`/`.assign` handler, before acking the rebalance, is
    ///   well-defined: pause, seek and resume are ops on the same per-partition queue and are
    ///   applied in the order they were enqueued, so a seek issued here overrides the resume
    ///   position librdkafka recorded when it paused the assignment for the rebalance.
    public func seek(_ topics: KafkaTopicList, timeout: Duration = .seconds(10)) async throws {
        try ensureNotClosed()
        try await client.seek(topicPartitionList: topics.list, timeout: timeout)
    }
}

extension KafkaConsumerStream: KafkaHandleProviding {
    public func withKafkaHandlePointer<T>(_ body: (OpaquePointer) async throws -> T) async throws -> T {
        try await self.client.withKafkaHandlePointer(body)
    }
}
