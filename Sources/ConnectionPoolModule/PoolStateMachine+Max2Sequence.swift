@available(macOS 13.0, *)
extension PoolStateMachine {
    @usableFromInline
    struct Max2Sequence<Element>: Sequence {
        @usableFromInline
        /*private*/ var first: Element?
        @usableFromInline
        /*private*/ var second: Element?

        @inlinable
        var count: Int {
            var count = 0
            if self.first != nil { count += 1 }
            if self.second != nil { count += 1 }
            return count
        }

        @inlinable
        static func empty() -> Self {
            Self.init(nil, nil)
        }

        @inlinable
        init(_ first: Element?, _ second: Element? = nil) {
            if let first = first {
                self.first = first
                self.second = second
            } else {
                self.first = second
                self.second = nil
            }
        }

        @inlinable
        init() {
            self.first = nil
            self.second = nil
        }

        @inlinable
        func makeIterator() -> Iterator {
            Iterator(first: self.first, second: self.second)
        }

        @usableFromInline
        struct Iterator: IteratorProtocol {
            @usableFromInline
            let first: Element?
            @usableFromInline
            let second: Element?

            @usableFromInline
            private(set) var index: UInt8 = 0

            @inlinable
            init(first: Element?, second: Element?) {
                self.first = first
                self.second = second
                self.index = 0
            }

            @inlinable
            mutating func next() -> Element? {
                switch self.index {
                case 0:
                    self.index += 1
                    return self.first
                case 1:
                    self.index += 1
                    return self.second
                default:
                    return nil
                }
            }
        }

        @inlinable
        mutating func append(_ element: Element) {
            precondition(self.second == nil)
            if self.first == nil {
                self.first = element
            } else {
                self.second = element
            }
        }

        @inlinable
        func map<NewElement>(_ transform: (Element) throws -> (NewElement)) rethrows -> Max2Sequence<NewElement> {
            try Max2Sequence<NewElement>(self.first.flatMap(transform), self.second.flatMap(transform))
        }
    }
}

@available(macOS 13.0, *)
extension PoolStateMachine.Max2Sequence: Equatable where Element: Equatable {}
