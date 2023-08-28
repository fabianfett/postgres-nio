import DequeModule

@available(macOS 13.0, *)
extension PoolStateMachine {

    @usableFromInline
    struct RequestQueue {
        @usableFromInline
        private(set) var generalPurposeQueue: Deque<RequestID>

        ///
        @usableFromInline
        private(set) var requests: [RequestID: Request]

        @inlinable
        var count: Int {
            self.requests.count
        }

        @inlinable
        var isEmpty: Bool {
            self.count == 0
        }

        @usableFromInline
        init() {
            self.generalPurposeQueue = .init(minimumCapacity: 256)
            self.requests = .init(minimumCapacity: 256)
        }

        @inlinable
        mutating func queue(_ request: Request) {
            self.requests[request.id] = request
            self.generalPurposeQueue.append(request.id)
        }

        @inlinable
        mutating func pop() -> Request? {
            while let requestID = self.generalPurposeQueue.popFirst() {
                if let requestIndex = self.requests.index(forKey: requestID) {
                    return self.requests.remove(at: requestIndex).value
                }
            }

            return nil
        }

        @inlinable
        mutating func remove(_ requestID: RequestID) -> Request? {
            self.requests.removeValue(forKey: requestID)
        }

        @inlinable
        mutating func removeAll() -> RequestCollection<Request> {
            let result = RequestCollection(self.requests.values)
            self.requests.removeAll()
            self.generalPurposeQueue.removeAll()
            return result
        }
    }
}
