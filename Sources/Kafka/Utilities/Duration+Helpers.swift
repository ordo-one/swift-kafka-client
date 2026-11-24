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

extension Duration {
    internal var inMilliseconds: UInt {
        let seconds = Double(components.seconds) * 1000.0
        let attoseconds = Double(components.attoseconds) * 1e-15
        return UInt(seconds + attoseconds)
    }

    /// Whole milliseconds, rounded toward zero.
    /// Integer arithmetic, because at the size of a time since the epoch a `Double` sum can round up to the next millisecond.
    internal var wholeMilliseconds: Int64 {
        components.seconds * 1000 + components.attoseconds / 1_000_000_000_000_000
    }

    internal var canBeRepresentedAsMilliseconds: Bool {
        return self.inMilliseconds > 0
    }
}
