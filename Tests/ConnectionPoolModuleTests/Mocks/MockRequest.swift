import ConnectionPoolModule

final class MockRequest: ConnectionRequestProtocol, Equatable, Sendable {
    typealias Connection = MockConnection

    struct ID: Hashable {
        var objectID: ObjectIdentifier

        init(_ request: MockRequest) {
            self.objectID = ObjectIdentifier(request)
        }
    }

    var id: ID { ID(self) }


    static func ==(lhs: MockRequest, rhs: MockRequest) -> Bool {
        lhs.id == rhs.id
    }

    func complete(with: Result<Connection, PoolError>) {

    }
}
