import class Foundation.JSONEncoder
import class Foundation.JSONDecoder
import NIOFoundationCompat

/// A type that is used to encode `Encodable` types as json into `ByteBuffer`.
public protocol PSQLJSONEncoder {
    
    /// Writes a JSON-encoded representation of the value you supply into the supplied `ByteBuffer`.
    ///
    /// - parameters:
    ///     - value: The value to encode as JSON.
    ///     - buffer: The `ByteBuffer` to encode into.
    func encode<T: Encodable>(_ value: T, into buffer: inout ByteBuffer) throws
}

/// A type that is used to decode `Decodable` types from a `ByteBuffer` containing json.
public protocol PSQLJSONDecoder {
    
    /// Returns a value of the type you specify, decoded from a JSON object inside the readable bytes of a `ByteBuffer`.
    ///
    /// If the `ByteBuffer` does not contain valid JSON, this method throws the
    /// `DecodingError.dataCorrupted(_:)` error. If a value within the JSON
    /// fails to decode, this method throws the corresponding error.
    ///
    /// - note: The provided `ByteBuffer` remains unchanged, neither the `readerIndex` nor the `writerIndex` will move.
    ///
    /// - parameters:
    ///     - type: The type of the value to decode from the supplied JSON object.
    ///     - buffer: The `ByteBuffer` that contains JSON object to decode.
    /// - returns: The decoded object.
    func decode<T: Decodable>(_ type: T.Type, from buffer: ByteBuffer) throws -> T
}

extension JSONEncoder: PSQLJSONEncoder {}
extension JSONDecoder: PSQLJSONDecoder {}

