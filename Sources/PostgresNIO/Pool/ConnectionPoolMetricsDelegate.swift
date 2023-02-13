
public protocol ConnectionPoolMetricsDelegate {
    associatedtype ConnectionID: Hashable

    /// The connection with the given ID has started trying to establish a connection. The outcome
    /// of the connection will be reported as either ``connectSucceeded(id:streamCapacity:)`` or
    /// ``connectFailed(id:error:)``.
    func startedConnecting(id: ConnectionID)

    /// A connection attempt failed with the given error. After some period of
    /// time ``startedConnecting(id:)`` may be called again.
    func connectFailed(id: ConnectionID, error: Error)

    /// A connection was established on the connection with the given ID. `streamCapacity` streams are
    /// available to use on the connection. The maximum number of available streams may change over
    /// time and is reported via ``connectionUtilizationChanged(id:streamsUsed:streamCapacity:)``. The
    func connectSucceeded(id: ConnectionID)

    /// The utlization of the connection changed; a stream may have been used, returned or the
    /// maximum number of concurrent streams available on the connection changed.
    func connectionLeased(id: ConnectionID)

    func connectionReleased(id: ConnectionID)

    /// The remote peer is quiescing the connection: no new streams will be created on it. The
    /// connection will eventually be closed and removed from the pool.
    func connectionClosing(id: ConnectionID)

    /// The connection was closed. The connection may be established again in the future (notified
    /// via ``startedConnecting(id:)``).
    func connectionClosed(id: ConnectionID, error: Error?)

    func requestQueueDepthChanged(_ newDepth: Int)
}

extension ConnectionPoolMetricsDelegate {
    func startedConnecting(id: ConnectionID) {}

    func connectFailed(id: ConnectionID, error: Error) {}

    func connectSucceeded(id: ConnectionID) {}

    func connectionLeased(id: ConnectionID) {}

    func connectionReleased(id: ConnectionID) {}

    func connectionClosing(id: ConnectionID) {}

    func connectionClosed(id: ConnectionID, error: Error?) {}

    func requestQueueDepthChanged(_ newDepth: Int) {}
}

struct NoOpConnectionPoolMetrics<ConnectionID: Hashable>: ConnectionPoolMetricsDelegate {
    init(connectionIDType: ConnectionID.Type) {}
}
