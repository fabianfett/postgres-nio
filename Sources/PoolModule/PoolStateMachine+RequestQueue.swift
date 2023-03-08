import DequeModule
import NIOCore

extension PoolStateMachine {

    @usableFromInline
    struct RequestQueue {
        @usableFromInline
        struct EventLoopQueue {
            @usableFromInline let maxConsecutivePicks: UInt8
            @usableFromInline private(set) var consecutivePicks: UInt8 = 0
            @usableFromInline private(set) var queue: Deque<RequestID>

            @inlinable
            init(maxConsecutivePicks: UInt8) {
                self.maxConsecutivePicks = maxConsecutivePicks
                self.queue = .init(minimumCapacity: 128)
            }

            @inlinable
            mutating func pop(lookup: inout [RequestID: Request]) -> Request? {
                guard self.consecutivePicks < self.maxConsecutivePicks else {
                    self.consecutivePicks = 0
                    return nil
                }

                while let requestID = self.queue.popFirst() {
                    if let requestIndex = lookup.index(forKey: requestID) {
                        let request = lookup.remove(at: requestIndex).value
                        self.consecutivePicks += 1
                        return request
                    }
                }

                self.consecutivePicks = 0
                return nil
            }

            @inlinable
            mutating func pop(id: RequestID) {
                precondition(self.queue.popFirst() == id)
            }

            @inlinable
            mutating func append(_ requestID: RequestID) {
                self.queue.append(requestID)
            }
        }

        @usableFromInline
        private(set) var generalPurposeQueue: Deque<RequestID>

        ///
        @usableFromInline
        private(set) var eventLoopQueues: [EventLoopID: EventLoopQueue]

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
        init(eventLoopGroup: EventLoopGroup, maxConsecutivePicksFromEventLoopQueue: UInt8) {
            self.generalPurposeQueue = .init(minimumCapacity: 128)
            self.eventLoopQueues = [:]

            let eventLoopIterator = eventLoopGroup.makeIterator()
            for eventLoop in eventLoopIterator {
                self.eventLoopQueues[.init(eventLoop)] = .init(maxConsecutivePicks: maxConsecutivePicksFromEventLoopQueue)
            }

            self.requests = .init(minimumCapacity: 256)
        }

        @inlinable
        mutating func queue(_ request: Request) {
            self.requests[request.id] = request
            self.generalPurposeQueue.append(request.id)
            if let eventLoop = request.preferredEventLoop {
                self.eventLoopQueues[.init(eventLoop)]!.append(request.id)
            }
        }

        @inlinable
        mutating func pop(for eventLoopID: EventLoopID) -> Request? {
            if let request = self.eventLoopQueues[eventLoopID]!.pop(lookup: &self.requests) {
                return request
            }

            while let requestID = self.generalPurposeQueue.popFirst() {
                if let requestIndex = self.requests.index(forKey: requestID) {
                    let request = self.requests.remove(at: requestIndex).value
                    if let preferredEL = request.preferredEventLoop {
                        // if we pop a request of the general purpose queue and it has a preferred
                        // el, we know it must be first in the specific EL queue. Let's remove it
                        // there right away.
                        self.eventLoopQueues[preferredEL.id]!.pop(id: requestID)
                    }
                    return request
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
            self.eventLoopQueues.removeAll()
            return result
        }
    }
}
