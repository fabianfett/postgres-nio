import XCTest
import NIOCore
@testable import PostgresNIO

class SSLRequestTests: XCTestCase {
    
    func testSSLRequest() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        encoder.ssl()
        var byteBuffer = encoder.flushBuffer()
        
        let byteBufferLength = Int32(byteBuffer.readableBytes)
        XCTAssertEqual(byteBufferLength, byteBuffer.readInteger())
        XCTAssertEqual(PostgresFrontendMessageEncoder.sslRequestCode, byteBuffer.readInteger())

        XCTAssertEqual(byteBuffer.readableBytes, 0)
    }
    
}
