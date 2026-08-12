/// A type that can lend out its librdkafka handle for the duration of a call.
///
/// Lets ``KafkaTransaction`` commit offsets for either consumer flavour: the two reach their handle
/// differently — ``KafkaConsumer`` through a state machine, ``KafkaConsumerStream`` from a stored client.
/// - Warning: Do not escape the pointer from the closure for later use.
public protocol KafkaHandleProviding: Sendable {
    func withKafkaHandlePointer<T>(_ body: (OpaquePointer) async throws -> T) async throws -> T
}
