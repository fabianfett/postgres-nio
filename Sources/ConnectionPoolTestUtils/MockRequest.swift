import _ConnectionPoolModule

public final class MockRequest: ConnectionRequestProtocol, Hashable, Sendable {
    public typealias Connection = MockConnection

    public struct ID: Hashable {
        var objectID: ObjectIdentifier

        init(_ request: MockRequest) {
            self.objectID = ObjectIdentifier(request)
        }
    }

    public var id: ID { ID(self) }

    public init() {}

    public static func ==(lhs: MockRequest, rhs: MockRequest) -> Bool {
        lhs.id == rhs.id
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(self.id)
    }

    public func complete(with: Result<Connection, ConnectionPoolError>) {

    }
}