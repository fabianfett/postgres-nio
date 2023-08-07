import NIOCore
import PostgresNIO

public struct PostgresFrontendMessageDecoder: NIOSingleStepByteToMessageDecoder {
    public typealias InboundOut = PostgresFrontendMessage

    private(set) var isInStartup: Bool
    
    public init() {
        self.isInStartup = true
    }
    
    mutating public func decode(buffer: inout ByteBuffer) throws -> PostgresFrontendMessage? {
        // make sure we have at least one byte to read
        guard buffer.readableBytes > 0 else {
            return nil
        }
        
        if self.isInStartup {
            guard let length = buffer.getInteger(at: buffer.readerIndex, as: UInt32.self) else {
                return nil
            }
            
            guard var messageSlice = buffer.getSlice(at: buffer.readerIndex + 4, length: Int(length) - 4) else {
                return nil
            }
            buffer.moveReaderIndex(to: Int(length))
            let finalIndex = buffer.readerIndex
            
            guard let code = messageSlice.readInteger(as: UInt32.self) else {
                throw PostgresFrontendMessageDecodingError(.fieldNotDecodable, buffer: buffer)
            }
            
            switch code {
            case 80877103:
                self.isInStartup = true
                return .sslRequest
                
            case 196608:
                var user: String?
                var database: String?
                var options: String?
                
                while let name = messageSlice.readNullTerminatedString(), messageSlice.readerIndex < finalIndex {
                    let value = messageSlice.readNullTerminatedString()
                    
                    switch name {
                    case "user":
                        user = value
                        
                    case "database":
                        database = value
                        
                    case "options":
                        options = value
                        
                    default:
                        break
                    }
                }
                
                let parameters = PostgresFrontendMessage.Startup.Parameters(
                    user: user!,
                    database: database,
                    options: options,
                    replication: .false
                )
                
                let startup = PostgresFrontendMessage.Startup(
                    protocolVersion: 0x00_03_00_00,
                    parameters: parameters
                )
                
                precondition(buffer.readerIndex == finalIndex)
                self.isInStartup = false
                
                return .startup(startup)
                
            default:
                throw PostgresFrontendMessageDecodingError(.unknownStartupCode(code), buffer: buffer)
            }
        }
        
        // all other packages have an Int32 after the identifier that determines their length.
        // do we have enough bytes for that?
        guard let idByte = buffer.getInteger(at: buffer.readerIndex, as: UInt8.self),
              let length = buffer.getInteger(at: buffer.readerIndex + 1, as: Int32.self) else {
            return nil
        }
        
        // At this point we are sure, that we have enough bytes to decode the next message.
        // 1. Create a byteBuffer that represents exactly the next message. This can be force
        //    unwrapped, since it was verified that enough bytes are available.
        guard let completeMessageBuffer = buffer.readSlice(length: 1 + Int(length)) else {
            return nil
        }
        
        // 2. make sure we have a known message identifier
        guard let messageID = PostgresFrontendMessage.ID(rawValue: idByte) else {
            throw PostgresFrontendMessageDecodingError(.unknownMessageID(idByte))
        }
        
        // 3. decode the message
        do {
            // get a mutable byteBuffer copy
            var slice = completeMessageBuffer
            // move reader index forward by five bytes
            slice.moveReaderIndex(forwardBy: 5)
            
            return try PostgresFrontendMessage.decode(from: &slice, for: messageID)
        } catch var error as PostgresFrontendMessageDecodingError {
            error.buffer = buffer
            throw error
        } catch {
            preconditionFailure("Expected to only see `PartialDecodingError`s here.")
        }
    }
    
    mutating public func decodeLast(buffer: inout ByteBuffer, seenEOF: Bool) throws -> PostgresFrontendMessage? {
        try self.decode(buffer: &buffer)
    }
}

extension PostgresFrontendMessage {
    
    static func decode(from buffer: inout ByteBuffer, for messageID: ID) throws -> PostgresFrontendMessage {
        switch messageID {
        case .bind:
            guard let portalName = buffer.readNullTerminatedString() else {
                throw PostgresFrontendMessageDecodingError(.fieldNotDecodable)
            }
            guard let preparedStatementName = buffer.readNullTerminatedString() else {
                throw PostgresFrontendMessageDecodingError(.fieldNotDecodable)
            }
            guard let parameterFormatCount = buffer.readInteger(as: UInt16.self) else {
                throw PostgresFrontendMessageDecodingError(.fieldNotDecodable)
            }

            let parameterFormats = try (0..<parameterFormatCount).map { _ in
                guard let code = buffer.readInteger(as: Int16.self) else {
                    throw PostgresFrontendMessageDecodingError(.missingBytes)
                }
                guard let format = PostgresFormat(rawValue: code) else {
                    throw PostgresFrontendMessageDecodingError(.unknownPostgresFormat(code))
                }
                return format
            }

            guard let parameterCount = buffer.readInteger(as: UInt16.self) else {
                throw PostgresFrontendMessageDecodingError(.fieldNotDecodable)
            }

            let parameters = try (0..<parameterCount).map { _ throws -> ByteBuffer? in
                let length = buffer.readInteger(as: UInt16.self)
                switch length {
                case .some(..<0):
                    return nil
                case .some(let length):
                    return buffer.readSlice(length: Int(length))
                case .none:
                    throw PostgresFrontendMessageDecodingError(.missingBytes)
                }
            }

            guard let resultColumnFormatCount = buffer.readInteger(as: UInt16.self) else {
                preconditionFailure("TODO: Unimplemented")
            }

            let resultColumnFormats = try (0..<resultColumnFormatCount).map { _ in
                guard let code = buffer.readInteger(as: Int16.self) else {
                    throw PostgresFrontendMessageDecodingError(.missingBytes)
                }
                guard let format = PostgresFormat(rawValue: code) else {
                    throw PostgresFrontendMessageDecodingError(.unknownPostgresFormat(code))
                }
                return format
            }

            return .bind(
                Bind(
                    portalName: portalName,
                    preparedStatementName: preparedStatementName,
                    parameterFormats: parameterFormats,
                    parameters: parameters,
                    resultColumnFormats: resultColumnFormats
                )
            )

        case .close:
            switch buffer.readInteger(as: UInt8.self) {
            case UInt8(ascii: "S"):
                guard let string = buffer.readNullTerminatedString() else { throw PostgresFrontendMessageDecodingError(.missingBytes) }
                return .describe(.preparedStatement(string))
            case UInt8(ascii: "P"):
                guard let string = buffer.readNullTerminatedString() else { throw PostgresFrontendMessageDecodingError(.missingBytes) }
                return .close(.preparedStatement(string))
            case .some(let code):
                throw PostgresFrontendMessageDecodingError(.unknownDescribeCode(code))
            case .none:
                throw PostgresFrontendMessageDecodingError(.missingBytes)
            }

        case .describe:
            switch buffer.readInteger(as: UInt8.self) {
            case UInt8(ascii: "S"):
                guard let string = buffer.readNullTerminatedString() else { throw PostgresFrontendMessageDecodingError(.missingBytes) }
                return .describe(.preparedStatement(string))
            case UInt8(ascii: "P"):
                guard let string = buffer.readNullTerminatedString() else { throw PostgresFrontendMessageDecodingError(.missingBytes) }
                return .describe(.portal(string))
            case .some(let code):
                throw PostgresFrontendMessageDecodingError(.unknownDescribeCode(code))
            case .none:
                throw PostgresFrontendMessageDecodingError(.missingBytes)
            }

        case .execute:
            guard let portalName = buffer.readNullTerminatedString() else {
                throw PostgresFrontendMessageDecodingError(.fieldNotDecodable)
            }

            guard let maxNumberOfRows = buffer.readInteger(as: Int32.self) else {
                throw PostgresFrontendMessageDecodingError(.fieldNotDecodable)
            }

            return .execute(.init(portalName: portalName, maxNumberOfRows: maxNumberOfRows))

        case .flush:
            return .flush

        case .parse:
            guard let preparedStatementName = buffer.readNullTerminatedString() else {
                throw PostgresFrontendMessageDecodingError(.fieldNotDecodable)
            }
            guard let query = buffer.readNullTerminatedString() else {
                throw PostgresFrontendMessageDecodingError(.fieldNotDecodable)
            }
            guard let parameterCount = buffer.readInteger(as: UInt16.self) else {
                throw PostgresFrontendMessageDecodingError(.fieldNotDecodable)
            }
            let parameters = (0..<parameterCount).map({ _ in PostgresDataType(buffer.readInteger(as: UInt32.self)!) })
            return .parse(.init(preparedStatementName: preparedStatementName, query: query, parameters: parameters))

        case .password:
            guard let password = buffer.readNullTerminatedString() else {
                throw PostgresFrontendMessageDecodingError(.fieldNotDecodable)
            }
            return .password(.init(password))
        case .saslInitialResponse:
            throw PostgresFrontendMessageDecodingError(.unimplemented)
        case .saslResponse:
            throw PostgresFrontendMessageDecodingError(.unimplemented)
        case .sync:
            return .sync
        case .terminate:
            return .terminate
        }
    }
}

public struct PostgresFrontendMessageDecodingError: Error {
    enum Reason {
        case missingBytes
        case fieldNotDecodable
        case unknownMessageID(UInt8)
        case unknownStartupCode(UInt32)
        case unknownDescribeCode(UInt8)
        case unknownPostgresFormat(Int16)
        case unimplemented
    }

    var reason: Reason
    var file: String
    var line: Int
    var buffer: ByteBuffer

    init(_ reason: Reason, buffer: ByteBuffer = ByteBuffer(), file: String = #fileID, line: Int = #line) {
        self.reason = reason
        self.file = file
        self.line = line
        self.buffer = buffer
    }
}
