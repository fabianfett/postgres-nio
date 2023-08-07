import XCTest
import NIOCore
@testable import PostgresNIO

class PSQLFrontendMessageTests: XCTestCase {
    
    // MARK: ID
    
    func testMessageIDs() {
        XCTAssertEqual(PostgresFrontendMessageID.bind.rawValue, UInt8(ascii: "B"))
        XCTAssertEqual(PostgresFrontendMessageID.close.rawValue, UInt8(ascii: "C"))
        XCTAssertEqual(PostgresFrontendMessageID.describe.rawValue, UInt8(ascii: "D"))
        XCTAssertEqual(PostgresFrontendMessageID.execute.rawValue, UInt8(ascii: "E"))
        XCTAssertEqual(PostgresFrontendMessageID.flush.rawValue, UInt8(ascii: "H"))
        XCTAssertEqual(PostgresFrontendMessageID.parse.rawValue, UInt8(ascii: "P"))
        XCTAssertEqual(PostgresFrontendMessageID.password.rawValue, UInt8(ascii: "p"))
        XCTAssertEqual(PostgresFrontendMessageID.sync.rawValue, UInt8(ascii: "S"))
        XCTAssertEqual(PostgresFrontendMessageID.terminate.rawValue, UInt8(ascii: "X"))
    }
    
    // MARK: Encoder
    
    func testEncodeFlush() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        encoder.flush()
        var byteBuffer = encoder.flushBuffer()

        XCTAssertEqual(byteBuffer.readableBytes, 5)
        XCTAssertEqual(PostgresFrontendMessageID.flush.rawValue, byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual(4, byteBuffer.readInteger(as: Int32.self)) // payload length
    }
    
    func testEncodeSync() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        encoder.sync()
        var byteBuffer = encoder.flushBuffer()

        XCTAssertEqual(byteBuffer.readableBytes, 5)
        XCTAssertEqual(PostgresFrontendMessageID.sync.rawValue, byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual(4, byteBuffer.readInteger(as: Int32.self)) // payload length
    }
    
    func testEncodeTerminate() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        encoder.terminate()
        var byteBuffer = encoder.flushBuffer()

        XCTAssertEqual(byteBuffer.readableBytes, 5)
        XCTAssertEqual(PostgresFrontendMessageID.terminate.rawValue, byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual(4, byteBuffer.readInteger(as: Int32.self)) // payload length
    }

}
