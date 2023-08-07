import NIOCore
import PostgresNIO

public struct PostgresBackendMessageEncoder {
    private enum State {
        case flushed
        case writable
    }

    private var buffer: ByteBuffer
    private var state: State = .writable

    public init() {
        self.init(minimumCapacity: 1024)
    }

    public init(minimumCapacity: Int) {
        self.buffer = .init()
        self.buffer.reserveCapacity(minimumCapacity)
    }

    public mutating func flushBuffer() -> ByteBuffer {
        self.state = .flushed
        return self.buffer
    }

    public mutating func encodeAuthenticationOK() {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .authentication, length: 4, Int32(0))
    }

    public mutating func encodeAuthenticationKerberosV5() {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .authentication, length: 4, Int32(2))
    }

    public mutating func encodeAuthenticationPlaintext() {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .authentication, length: 4, Int32(3))
    }

    public mutating func encodeAuthenticationMD5(salt: UInt32) {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .authentication, length: 8, Int32(5), salt)
    }

    public mutating func encodeAuthenticationSCMCredential() {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .authentication, length: 4, Int32(6))
    }

    public mutating func encodeAuthenticationGSS() {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .authentication, length: 4, Int32(7))
    }

    public mutating func encodeAuthenticationGSSContinue<Bytes: Collection>(_ bytes: Bytes) where Bytes.Element == UInt8 {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .authentication, length: UInt32(4 + bytes.count), Int32(8))
        self.buffer.writeBytes(bytes)
    }

    public mutating func encodeAuthenticationSSPI() {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .authentication, length: 4, Int32(9))
    }

    public mutating func encodeAuthenticationSASL(names: [String]) {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(
            id: .authentication,
            length: UInt32(names.reduce(0, { $0 + $1.utf8.count + 1 }) + 4),
            Int32(10)
        )
        for name in names {
            buffer.writeNullTerminatedString(name)
        }
    }

    public mutating func encodeAuthenticationSASLContinue<Bytes: Collection>(_ bytes: Bytes) where Bytes.Element == UInt8 {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .authentication, length: UInt32(4 + bytes.count), Int32(11))
        self.buffer.writeBytes(bytes)
    }

    public mutating func encodeAuthenticationSASLFinal<Bytes: Collection>(_ bytes: Bytes) where Bytes.Element == UInt8 {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .authentication, length: UInt32(4 + bytes.count), Int32(12))
        self.buffer.writeBytes(bytes)
    }

    public mutating func encodeBackendKeyData(processID: Int32, secretKey: Int32) {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .authentication, length: 12, processID, secretKey)
    }

    public mutating func encodeBindComplete() {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .bindComplete, length: 4)
    }

    public mutating func encodeCloseComplete() {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .closeComplete, length: 4)
    }

    public mutating func encodeEmptyQueryResponse() {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .emptyQueryResponse, length: 4)
    }

    public mutating func encodeNoData() {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .noData, length: 4)
    }

    public mutating func encodeParseComplete() {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .parseComplete, length: 4)
    }

    public mutating func encodePortalSuspended() {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .portalSuspended, length: 4)
    }

    public mutating func commandComplete(tag: String) {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .commandComplete, length: UInt32(4 + tag.utf8.count + 1))
        self.buffer.writeNullTerminatedString(tag)
    }

    public mutating func encodeDataRow(columnCount: Int16, bytes: ByteBuffer) {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .dataRow, length: UInt32(4 + 2 + bytes.readableBytes), columnCount)
        self.buffer.writeBytes(bytes.readableBytesView)
    }

    public mutating func encodeSSLSupported() {
        self.clearIfNeeded()
        self.buffer.writeInteger(UInt8(ascii: "S"))
    }

    public mutating func encodeSSLUnsupported() {
        self.clearIfNeeded()
        self.buffer.writeInteger(UInt8(ascii: "N"))
    }

    public mutating func encodeReadyForQuery(transactionState: PostgresBackendMessage.TransactionState) {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .readyForQuery, length: 5, transactionState.rawValue)
    }

    public mutating func encodeNotificationResponse(backendPID: Int32, channel: String, payload: String) {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(
            id: .notificationResponse,
            length: UInt32(8 + channel.utf8.count + 1 + payload.utf8.count + 1),
            backendPID
        )
        self.buffer.writeNullTerminatedString(channel)
        self.buffer.writeNullTerminatedString(payload)
    }

    public mutating func encodeParameterDescription(_ parameters: [PostgresDataType]) {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(
            id: .parameterDescription,
            length: UInt32(2 + parameters.count * 4),
            Int16(parameters.count)
        )

        for parameter in parameters {
            self.buffer.writeInteger(parameter.rawValue)
        }
    }

    public mutating func encodeParameterStatus(_ parameter: String, value: String) {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(
            id: .parameterStatus,
            length: UInt32(4 + parameter.utf8.count + 1 + value.utf8.count + 1)
        )

        self.buffer.writeNullTerminatedString(parameter)
        self.buffer.writeNullTerminatedString(value)
    }

    public mutating func encodeErrorResponse(_ response: PostgresBackendMessage.ErrorResponse) {
        self.clearIfNeeded()
        self.buffer.writeInteger(PostgresBackendMessage.ID.error.rawValue)
        self.encodeLengthPrefixed { buffer in
            for (key, value) in response.fields {
                buffer.writeInteger(key.rawValue, as: UInt8.self)
                buffer.writeNullTerminatedString(value)
            }
            buffer.writeInteger(0, as: UInt8.self) // signal done
        }
    }

    public mutating func encodeNoticeResponse(_ response: PostgresBackendMessage.NoticeResponse) {
        self.clearIfNeeded()
        self.buffer.writeInteger(PostgresBackendMessage.ID.noticeResponse.rawValue)
        self.encodeLengthPrefixed { buffer in
            for (key, value) in response.fields {
                buffer.writeInteger(key.rawValue, as: UInt8.self)
                buffer.writeNullTerminatedString(value)
            }
            buffer.writeInteger(0, as: UInt8.self) // signal done
        }
    }

    public mutating func encodeRowDescription(_ rowDescription: PostgresBackendMessage.RowDescription) {
        self.clearIfNeeded()
        self.buffer.writeInteger(PostgresBackendMessage.ID.rowDescription.rawValue)
        self.encodeLengthPrefixed { buffer in
            buffer.writeInteger(Int16(rowDescription.columns.count))

            for column in rowDescription.columns {
                buffer.writeNullTerminatedString(column.name)
                buffer.writeMultipleIntegers(
                    column.tableOID,
                    column.columnAttributeNumber,
                    column.dataType.rawValue,
                    column.dataTypeSize,
                    column.dataTypeModifier,
                    column.format.rawValue
                )
            }
        }
    }

    private mutating func encodeLengthPrefixed(_ encode: (inout ByteBuffer) -> ()) {
        let startIndex = self.buffer.writerIndex
        self.buffer.writeInteger(UInt32(0)) // placeholder for length
        encode(&self.buffer)
        let length = UInt32(self.buffer.writerIndex - startIndex)
        self.buffer.setInteger(length, at: startIndex)
    }

    private mutating func clearIfNeeded() {
        switch self.state {
        case .flushed:
            self.state = .writable
            self.buffer.clear()

        case .writable:
            break
        }
    }
}

extension ByteBuffer {
    mutating fileprivate func psqlWriteMultipleIntegers(id: PostgresBackendMessage.ID, length: UInt32) {
        self.writeMultipleIntegers(id.rawValue, 4 + length)
    }

    mutating fileprivate func psqlWriteMultipleIntegers<T1: FixedWidthInteger>(id: PostgresBackendMessage.ID, length: UInt32, _ t1: T1) {
        self.writeMultipleIntegers(id.rawValue, 4 + length, t1)
    }

    mutating fileprivate func psqlWriteMultipleIntegers<T1: FixedWidthInteger, T2: FixedWidthInteger>(id: PostgresBackendMessage.ID, length: UInt32, _ t1: T1, _ t2: T2) {
        self.writeMultipleIntegers(id.rawValue, 4 + length, t1, t2)
    }
}

public enum PostgresBackendMessage {
    public enum Field: UInt8, Hashable {
        case localizedSeverity = 0x53 /// S
        case severity = 0x56 /// V
        case sqlState = 0x43 /// C
        case message = 0x4D /// M
        case detail = 0x44 /// D
        case hint = 0x48 /// H
        case position = 0x50 /// P
        case internalPosition = 0x70 /// p
        case internalQuery = 0x71 /// q
        case locationContext = 0x57 /// W
        case schemaName = 0x73 /// s
        case tableName = 0x74 /// t
        case columnName = 0x63 /// c
        case dataTypeName = 0x64 /// d
        case constraintName = 0x6E /// n
        case file = 0x46 /// F
        case line = 0x4C /// L
        case routine = 0x52 /// R
    }

    public struct ErrorResponse {
        public var fields: [(PostgresBackendMessage.Field, String)]

        public init(fields: [(PostgresBackendMessage.Field, String)]) {
            self.fields = fields
        }
    }

    public struct NoticeResponse {
        public var fields: [(PostgresBackendMessage.Field, String)]

        public init(fields: [(PostgresBackendMessage.Field, String)]) {
            self.fields = fields
        }
    }

    public enum TransactionState: UInt8, Hashable {
        case idle = 73                      // ascii: I
        case inTransaction = 84             // ascii: T
        case inFailedTransaction = 69       // ascii: E
    }

    public struct RowDescription: Sendable, Hashable, ExpressibleByArrayLiteral {
        public typealias ArrayLiteralElement = Column

        public var columns: [Column]

        public init(columns: [Column]) {
            self.columns = columns
        }

        public init(arrayLiteral elements: ArrayLiteralElement...) {
            self.columns = elements
        }

        public struct Column: Hashable, Sendable {
            public var name: String
            public var tableOID: Int32
            public var columnAttributeNumber: Int16
            public var dataType: PostgresDataType
            public var dataTypeSize: Int16
            public var dataTypeModifier: Int32
            public var format: PostgresFormat

            public init(
                name: String,
                tableOID: Int32,
                columnAttributeNumber: Int16,
                dataType: PostgresDataType,
                dataTypeSize: Int16,
                dataTypeModifier: Int32,
                format: PostgresFormat
            ) {
                self.name = name
                self.tableOID = tableOID
                self.columnAttributeNumber = columnAttributeNumber
                self.dataType = dataType
                self.dataTypeSize = dataTypeSize
                self.dataTypeModifier = dataTypeModifier
                self.format = format
            }
        }
    }

    public enum ID: UInt8, Hashable {
        case authentication = 82            // ascii: R
        case backendKeyData = 75            // ascii: K
        case bindComplete = 50              // ascii: 2
        case closeComplete = 51             // ascii: 3
        case commandComplete = 67           // ascii: C
        case copyData = 100                 // ascii: d
        case copyDone = 99                  // ascii: c
        case copyInResponse = 71            // ascii: G
        case copyOutResponse = 72           // ascii: H
        case copyBothResponse = 87          // ascii: W
        case dataRow = 68                   // ascii: D
        case emptyQueryResponse = 73        // ascii: I
        case error = 69                     // ascii: E
        case functionCallResponse = 86      // ascii: V
        case negotiateProtocolVersion = 118 // ascii: v
        case noData = 110                   // ascii: n
        case noticeResponse = 78            // ascii: N
        case notificationResponse = 65      // ascii: A
        case parameterDescription = 116     // ascii: t
        case parameterStatus = 83           // ascii: S
        case parseComplete = 49             // ascii: 1
        case portalSuspended = 115          // ascii: s
        case readyForQuery = 90             // ascii: Z
        case rowDescription = 84            // ascii: T
    }
}
