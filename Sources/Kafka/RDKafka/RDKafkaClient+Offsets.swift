import Crdkafka
import Foundation

/// Offset lookups that need no subscription: what another consumer group has committed and
/// where the partition currently ends. Together they give that group's lag as seen by the broker.
extension RDKafkaClient {
    /// Committed offsets for `group` on the given partitions. An entry's offset is
    /// `RD_KAFKA_OFFSET_INVALID` when the group has never committed to that partition.
    public func listConsumerGroupOffsets(
        group: String,
        topicPartitions: [(topic: String, partition: KafkaPartition)],
        timeout: Duration
    ) async throws -> [TopicPartition] {
        let list = RDKafkaTopicPartitionList(size: Int32(topicPartitions.count))
        for (topic, partition) in topicPartitions {
            list.add(topic: topic, partition: partition)
        }

        return try await performBlockingCall(queue: self.gcdQueue) {
            try self.withKafkaHandlePointer { kafkaHandle in
                let timeoutMs = Int32(max(timeout, .zero).inMilliseconds)

                let adminOptions = rd_kafka_AdminOptions_new(kafkaHandle, RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS)
                defer { rd_kafka_AdminOptions_destroy(adminOptions) }

                let errorChars = UnsafeMutablePointer<CChar>.allocate(capacity: RDKafkaClient.stringSize)
                defer { errorChars.deallocate() }
                let optionsError = rd_kafka_AdminOptions_set_request_timeout(adminOptions, timeoutMs, errorChars, RDKafkaClient.stringSize)
                if optionsError != RD_KAFKA_RESP_ERR_NO_ERROR {
                    throw KafkaError.rdKafkaError(wrapping: optionsError, errorMessage: String(cString: errorChars))
                }

                let request = list.withListPointer { rd_kafka_ListConsumerGroupOffsets_new(group, $0) }
                defer { rd_kafka_ListConsumerGroupOffsets_destroy(request) }

                let resultQueue = rd_kafka_queue_new(kafkaHandle)
                defer { rd_kafka_queue_destroy(resultQueue) }

                var requests: [OpaquePointer?] = [request]
                requests.withUnsafeMutableBufferPointer {
                    rd_kafka_ListConsumerGroupOffsets(kafkaHandle, $0.baseAddress, 1, adminOptions, resultQueue)
                }

                guard let resultEvent = rd_kafka_queue_poll(resultQueue, timeoutMs) else {
                    throw KafkaError.rdKafkaError(errorMessage: "rd_kafka_queue_poll() timed out")
                }
                defer { rd_kafka_event_destroy(resultEvent) }

                let eventError = rd_kafka_event_error(resultEvent)
                if eventError != RD_KAFKA_RESP_ERR_NO_ERROR {
                    throw KafkaError.rdKafkaError(wrapping: eventError, errorMessage: String(cString: rd_kafka_event_error_string(resultEvent)))
                }

                var groupCount: size_t = 0
                guard let result = rd_kafka_event_ListConsumerGroupOffsets_result(resultEvent),
                      let groups = rd_kafka_ListConsumerGroupOffsets_result_groups(result, &groupCount),
                      groupCount == 1,
                      let groupResult = groups[0] else {
                    throw KafkaError.rdKafkaError(errorMessage: "ListConsumerGroupOffsets returned no group result")
                }

                if let groupError = rd_kafka_group_result_error(groupResult) {
                    throw KafkaError.rdKafkaError(
                        wrapping: rd_kafka_error_code(groupError),
                        errorMessage: String(cString: rd_kafka_error_string(groupError))
                    )
                }

                guard let partitions = rd_kafka_group_result_partitions(groupResult) else { return [] }
                return (0 ..< Int(partitions.pointee.cnt)).map { index in
                    let element = partitions.pointee.elems[index]
                    return TopicPartition(
                        String(cString: element.topic),
                        KafkaPartition(rawValue: Int(element.partition)),
                        KafkaOffset(rawValue: Int(element.offset))
                    )
                }
            }
        }
    }

    /// Low and high watermarks of one partition as the broker reports them now.
    public func queryWatermarkOffsets(
        topic: String,
        partition: KafkaPartition,
        timeout: Duration
    ) async throws -> (low: KafkaOffset, high: KafkaOffset) {
        try await performBlockingCall(queue: self.gcdQueue) {
            try self.withKafkaHandlePointer { kafkaHandle in
                var low: Int64 = 0
                var high: Int64 = 0
                let result = rd_kafka_query_watermark_offsets(
                    kafkaHandle, topic, Int32(partition.rawValue), &low, &high, Int32(max(timeout, .zero).inMilliseconds)
                )
                if result != RD_KAFKA_RESP_ERR_NO_ERROR {
                    throw KafkaError.rdKafkaError(wrapping: result)
                }
                return (KafkaOffset(rawValue: Int(low)), KafkaOffset(rawValue: Int(high)))
            }
        }
    }
}
