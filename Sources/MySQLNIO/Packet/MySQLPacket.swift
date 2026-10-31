import NIOCore

public struct MySQLPacket {
    public var payload: ByteBuffer
    
    public init(payload: ByteBuffer) {
        self.payload = payload
    }
    
    public init() {
        self.payload = ByteBufferAllocator().buffer(capacity: 0)
    }
    
    public var isError: Bool {
        self.headerFlag == 0xFF
    }
    
    public var isOK: Bool {
        self.headerFlag == 0x00
    }
    
    public var isEOF: Bool {
        // A text row whose first value is 16 MiB or more starts with 0xFE too; the server keeps EOF and OK packets shorter than that.
        self.headerFlag == 0xFE && self.payload.readableBytes < 0xFF_FF_FF
    }
    
    var headerFlag: UInt8? {
        self.payload.getInteger(at: self.payload.readerIndex)
    }
}

extension MySQLPacket: CustomStringConvertible {
    public var description: String {
        let bytes = [UInt8](self.payload.readableBytesView)
        return "\(bytes.prefix(16))... (err: \(self.isError), ok: \(self.isOK), eof: \(self.isEOF))"
    }
}

public protocol MySQLPacketDecodable {
    static func decode(from packet: inout MySQLPacket, capabilities: MySQLProtocol.CapabilityFlags) throws -> Self
}

public protocol MySQLPacketEncodable {
    func encode(to packet: inout MySQLPacket, capabilities: MySQLProtocol.CapabilityFlags) throws
}

extension MySQLPacket {
    public mutating func decode<T: MySQLPacketDecodable>(_: T.Type, capabilities: MySQLProtocol.CapabilityFlags) throws -> T {
        try T.decode(from: &self, capabilities: capabilities)
    }
    
    public static func encode<T: MySQLPacketEncodable>(_ value: T, capabilities: MySQLProtocol.CapabilityFlags) throws -> MySQLPacket {
        var packet = MySQLPacket()
        try value.encode(to: &packet, capabilities: capabilities)
        return packet
    }
}
