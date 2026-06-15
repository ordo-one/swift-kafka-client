//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2023 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

// MARK: - RDKafkaStatistics

struct RDKafkaStatistics: Hashable, Codable {
    let queuedOperation: Int?
    let queuedProducerMessages: Int?
    let queuedProducerMessagesSize: Int?
    let topicsInMetadataCache: Int?
    let totalKafkaBrokerRequests: Int?
    let totalKafkaBrokerBytesSent: Int?
    let totalKafkaBrokerResponses: Int?
    let totalKafkaBrokerResponsesSize: Int?
    let totalKafkaBrokerMessagesSent: Int?
    let totalKafkaBrokerMessagesBytesSent: Int?
    let totalKafkaBrokerMessagesRecieved: Int?
    let totalKafkaBrokerMessagesBytesRecieved: Int?

    let topics: [String: Topic]?
    let brokers: [String: Broker]?

    enum CodingKeys: String, CodingKey {
        case topics
        case brokers
        case queuedOperation = "replyq"
        case queuedProducerMessages = "msg_cnt"
        case queuedProducerMessagesSize = "msg_size"
        case topicsInMetadataCache = "metadata_cache_cnt"
        case totalKafkaBrokerRequests = "tx"
        case totalKafkaBrokerBytesSent = "tx_bytes"
        case totalKafkaBrokerResponses = "rx"
        case totalKafkaBrokerResponsesSize = "rx_bytes"
        case totalKafkaBrokerMessagesSent = "txmsgs"
        case totalKafkaBrokerMessagesBytesSent = "txmsg_bytes"
        case totalKafkaBrokerMessagesRecieved = "rxmsgs"
        case totalKafkaBrokerMessagesBytesRecieved = "rxmsg_bytes"
    }

    var lag: Int? {
        var sumLag: Int?
        for (_, topic) in topics ?? [:] {
            guard let lag = topic.lag else { return nil }
            sumLag = (sumLag ?? 0) + lag
        }
        return sumLag
    }
}

extension RDKafkaStatistics {
    struct Topic: Hashable, Codable {
        let partitions: [String: Partition]?

        var lag: Int? {
            var sumLag: Int?
            for (name, partition) in partitions ?? [:] where name != "-1" {
                guard let lag = partition.lag else { return nil }
                sumLag = (sumLag ?? 0) + lag
            }
            return sumLag
        }
    }
}

extension RDKafkaStatistics {
    struct Partition: Hashable, Codable {
        let committedOffset: Int? // Last committed offset.
        let eofOffset: Int? // Last PARTITION_EOF signaled offset.
        let lastStableOffset: Int? // Partition's last stable offset on broker.
        let consumerLagStored: Int? // Difference between (hi_offset or ls_offset) and stored_offset.
        let desired: Bool? // Partition is explicitly desired by the application.
        let leaderBroker: Int? // Current leader broker id (-1 if unknown).

        enum CodingKeys: String, CodingKey {
            case committedOffset = "committed_offset"
            case eofOffset = "eof_offset"
            case lastStableOffset = "ls_offset"
            case consumerLagStored = "consumer_lag_stored"
            case desired
            case leaderBroker = "leader"
        }

        var lag: Int? {
            if lastStableOffset == 0 { // There is no commits to the partition
                return 0
            }

            // Sometimes there is no stored offset, and we should check that we read everything before our start
            if let committedOffset, committedOffset >= 0,
               let eofOffset, eofOffset >= 0,
               eofOffset - committedOffset == 1 { // commited one before eof
                return 0
            }

            if let consumerLagStored, consumerLagStored >= 0 {
                return consumerLagStored
            }

            return nil
        }
    }
}

extension RDKafkaStatistics {
    struct Broker: Hashable, Codable {
        enum State: String, Codable, Hashable {
            case initial = "INIT"
            case down = "DOWN"
            case tryConnect = "TRY_CONNECT"
            case connect = "CONNECT"
            case sslHandshake = "SSL_HANDSHAKE"
            case authLegacy = "AUTH_LEGACY"
            case up = "UP"
            case update = "UPDATE"
            case apiVersionQuery = "APIVERSION_QUERY"
            case authHandshake = "AUTH_HANDSHAKE"
            case authRequest = "AUTH_REQ"
            case reauth = "REAUTH"

            /// Returns true if the broker state indicates the Kafka protocol layer is operational
            var isOperational: Bool? {
                switch self {
                case .up, .update, .apiVersionQuery, .authHandshake, .authRequest, .reauth:
                    true
                case .down:
                    false
                default: // consider state as transient
                    nil
                }
            }
        }

        let nodeIdentifier: Int // Broker id (-1 for bootstraps).
        let state: State

        enum CodingKeys: String, CodingKey {
            case nodeIdentifier = "nodeid"
            case state
        }

        var isOperational: Bool? { state.isOperational }
    }
}

extension RDKafkaStatistics {
    /// The consumer can only fetch a partition through its current leader broker, so the cache is
    /// stale whenever a partition we consume has no operational leader — a broker that leads nothing
    /// we consume is irrelevant. A down leader resolves once Kafka re-elects from the ISR and
    /// librdkafka points `leader` at an operational broker; replica sets are not exposed by stats.
    var consumerHealthStatus: KafkaConsumerHealthStatus {
        guard let brokers else { return .healthy(lag: lag) }

        let operationalByNode = Dictionary(
            brokers.values.map { ($0.nodeIdentifier, $0.isOperational) },
            uniquingKeysWith: { first, _ in first }
        )

        var sawConsumedPartition = false
        for topic in topics ?? [:] {
            for (name, partition) in topic.value.partitions ?? [:]
            where name != "-1" && partition.desired == true {
                sawConsumedPartition = true
                let leader = partition.leaderBroker ?? -1
                if leader < 0 || operationalByNode[leader] != true {
                    return .stale
                }
            }
        }

        // Before partition assignment there is nothing to consume yet; fall back to requiring at
        // least one operational real broker so a fully down cluster is still reported as stale.
        if !sawConsumedPartition {
            let anyOperational = brokers.values.contains {
                $0.nodeIdentifier != -1 && $0.isOperational == true
            }
            return anyOperational ? .healthy(lag: lag) : .stale
        }

        return .healthy(lag: lag)
    }
}
