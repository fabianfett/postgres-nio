import DequeModule
import NIOCore

extension PoolStateMachine {

    struct RequestQueue {
        private struct EventLoopQueue {
            private let maxConsecutivePicks: UInt8
            private var consecutivePicks: UInt8 = 0
            private var queue: Deque<RequestID>

            init(maxConsecutivePicks: UInt8) {
                self.maxConsecutivePicks = maxConsecutivePicks
                self.queue = .init(minimumCapacity: 128)
            }

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

            mutating func pop(id: RequestID) {
                precondition(self.queue.popFirst() == id)
            }

            mutating func append(_ requestID: RequestID) {
                self.queue.append(requestID)
            }
        }

        ///
        private var generalPurposeQueue: Deque<RequestID>

        ///
        private var eventLoopQueues: [EventLoopID: EventLoopQueue]

        ///
        private var requests: [RequestID: Request]

        var count: Int {
            self.requests.count
        }

        var isEmpty: Bool {
            self.count == 0
        }

        init(eventLoopGroup: any EventLoopGroup, maxConsecutivePicksFromEventLoopQueue: UInt8) {
            self.generalPurposeQueue = .init(minimumCapacity: 128)
            self.eventLoopQueues = [:]

            let eventLoopIterator = eventLoopGroup.makeIterator()
            for eventLoop in eventLoopIterator {
                self.eventLoopQueues[.init(eventLoop)] = .init(maxConsecutivePicks: maxConsecutivePicksFromEventLoopQueue)
            }

            self.requests = .init(minimumCapacity: 256)
        }

        mutating func queue(_ request: Request) {
            self.requests[request.id] = request
            self.generalPurposeQueue.append(request.id)
            if let eventLoop = request.preferredEventLoop {
                self.eventLoopQueues[.init(eventLoop)]!.append(request.id)
            }
        }

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

        mutating func remove(_ requestID: RequestID) -> Request? {
            self.requests.removeValue(forKey: requestID)
        }

        mutating func removeAll() -> [Request] {
            let result = Array(self.requests.values)
            self.requests.removeAll()
            self.generalPurposeQueue.removeAll()
            self.eventLoopQueues.removeAll()
            return result
        }
    }
}
