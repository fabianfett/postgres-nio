import XCTest
import NIOCore
import NIOTestUtils
@testable import PostgresNIO
import PostgresTestUtils

class ParameterDescriptionTests: XCTestCase {
    
    func testDecode() {
        let expected: [PostgresDataType] = [.bool, .varchar, .uuid, .json, .jsonbArray]
        
        var encoder = PostgresBackendMessageEncoder()
        encoder.encodeParameterDescription(expected)
        
        XCTAssertNoThrow(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(encoder.flushBuffer(), [PostgresNIO.PostgresBackendMessage.parameterDescription(.init(dataTypes: expected))])],
            decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) }))
    }
    
    func testDecodeWithNegativeCount() {
        let dataTypes: [PostgresDataType] = [.bool, .varchar, .uuid, .json, .jsonbArray]
        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .parameterDescription) { buffer in
            buffer.writeInteger(Int16(-4))
            
            dataTypes.forEach { dataType in
                buffer.writeInteger(dataType.rawValue)
            }
        }
        
        XCTAssertThrowsError(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(buffer, [])],
            decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) })) {
            XCTAssert($0 is PostgresMessageDecodingError)
        }
    }
    
    func testDecodeColumnCountDoesntMatchMessageLength() {
        let dataTypes: [PostgresDataType] = [.bool, .varchar, .uuid, .json, .jsonbArray]
        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .parameterDescription) { buffer in
            // means three columns comming, but 5 are in the buffer actually.
            buffer.writeInteger(Int16(3))
            
            dataTypes.forEach { dataType in
                buffer.writeInteger(dataType.rawValue)
            }
        }
        
        XCTAssertThrowsError(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(buffer, [])],
            decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) })) {
            XCTAssert($0 is PostgresMessageDecodingError)
        }
    }
}
