import XCTest
import NIOCore
import NIOTestUtils
@testable import PostgresNIO
import PostgresTestUtils

class AuthenticationTests: XCTestCase {
    
    func testDecodeAuthentication() {
        var expected = [PostgresNIO.PostgresBackendMessage]()
        var encoder = PostgresBackendMessageEncoder()
        
        // add ok
        encoder.encodeAuthenticationOK()
        expected.append(.authentication(.ok))
        
        // add kerberos
        encoder.encodeAuthenticationKerberosV5()
        expected.append(.authentication(.kerberosV5))
        
        // add plaintext
        encoder.encodeAuthenticationPlaintext()
        expected.append(.authentication(.plaintext))
        
        // add md5
        let salt: UInt32 = 0x01_02_03_04
        encoder.encodeAuthenticationMD5(salt: salt)
        expected.append(.authentication(.md5(salt: salt)))

        // add scm credential
        encoder.encodeAuthenticationSCMCredential()
        expected.append(.authentication(.scmCredential))
        
        // add gss
        encoder.encodeAuthenticationGSS()
        expected.append(.authentication(.gss))
        
        // add sspi
        encoder.encodeAuthenticationSSPI()
        expected.append(.authentication(.sspi))
        
        XCTAssertNoThrow(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(encoder.flushBuffer(), expected)],
            decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: false) }
        ))
    }
}
