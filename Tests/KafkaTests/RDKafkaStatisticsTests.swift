//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2024 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import struct Foundation.Data
import class Foundation.JSONDecoder
@testable import Kafka
import XCTest

final class RDKafkaStatisticsTests: XCTestCase {
    private func healthStatus(fromJSON json: String) throws -> KafkaConsumerHealthStatus {
        let stats = try JSONDecoder().decode(RDKafkaStatistics.self, from: Data(json.utf8))
        return stats.consumerHealthStatus
    }

    func testConsumedPartitionWithOperationalLeaderIsHealthy() throws {
        let json = """
        {
          "brokers": { "b1": { "nodeid": 1, "state": "UP" } },
          "topics": { "t": { "partitions": { "0": { "desired": true, "leader": 1 } } } }
        }
        """
        XCTAssertEqual(try healthStatus(fromJSON: json), .healthy(lag: nil))
    }

    func testConsumedPartitionWithTransientLeaderIsStale() throws {
        // A persistently unreachable broker cycles TRY_CONNECT/CONNECT and is virtually never
        // sampled in DOWN — the previous "stale only if DOWN" mapping missed this.
        let json = """
        {
          "brokers": { "b1": { "nodeid": 1, "state": "TRY_CONNECT" } },
          "topics": { "t": { "partitions": { "0": { "desired": true, "leader": 1 } } } }
        }
        """
        XCTAssertEqual(try healthStatus(fromJSON: json), .stale)
    }

    func testConsumedPartitionWithoutLeaderIsStale() throws {
        let json = """
        {
          "brokers": { "b1": { "nodeid": 1, "state": "UP" } },
          "topics": { "t": { "partitions": { "0": { "desired": true, "leader": -1 } } } }
        }
        """
        XCTAssertEqual(try healthStatus(fromJSON: json), .stale)
    }

    func testDownBrokerLeadingNothingConsumedIsHealthy() throws {
        let json = """
        {
          "brokers": {
            "b1": { "nodeid": 1, "state": "UP" },
            "b2": { "nodeid": 2, "state": "TRY_CONNECT" }
          },
          "topics": { "t": { "partitions": {
            "0": { "desired": true, "leader": 1 },
            "1": { "desired": false, "leader": 2 }
          } } }
        }
        """
        XCTAssertEqual(try healthStatus(fromJSON: json), .healthy(lag: nil))
    }

    func testPartitionSpreadWithOneDownLeaderIsStale() throws {
        let json = """
        {
          "brokers": {
            "b1": { "nodeid": 1, "state": "UP" },
            "b2": { "nodeid": 2, "state": "CONNECT" }
          },
          "topics": { "t": { "partitions": {
            "0": { "desired": true, "leader": 1 },
            "1": { "desired": true, "leader": 2 }
          } } }
        }
        """
        XCTAssertEqual(try healthStatus(fromJSON: json), .stale)
    }

    func testNoConsumedPartitionsFallsBackToAnyOperationalBroker() throws {
        let upJSON = """
        { "brokers": { "b0": { "nodeid": -1, "state": "TRY_CONNECT" },
                       "b1": { "nodeid": 1, "state": "UP" } } }
        """
        XCTAssertEqual(try healthStatus(fromJSON: upJSON), .healthy(lag: nil))

        let downJSON = """
        { "brokers": { "b0": { "nodeid": -1, "state": "CONNECT" },
                       "b1": { "nodeid": 1, "state": "TRY_CONNECT" } } }
        """
        XCTAssertEqual(try healthStatus(fromJSON: downJSON), .stale)
    }
}
