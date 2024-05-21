import _ConnectionPoolModule
import XCTest
import _ConnectionPoolTestUtils

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class NoKeepAliveBehaviorTests: XCTestCase {
    func testNoKeepAlive() {
        let keepAliveBehavior = NoOpKeepAliveBehavior(connectionType: MockConnection.self)
        XCTAssertNil(keepAliveBehavior.keepAliveFrequency)
    }
}
