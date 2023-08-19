
public struct PoolError: Error, Hashable {
    enum Base: Error, Hashable {
        case requestTimeout
        case requestCancelled
        case poolShutdown
    }

    private let base: Base

    init(_ base: Base) { self.base = base }

    public static let requestTimeout = PoolError(.requestTimeout)
    public static let requestCancelled = PoolError(.requestCancelled)
    public static let poolShutdown = PoolError(.poolShutdown)
}
