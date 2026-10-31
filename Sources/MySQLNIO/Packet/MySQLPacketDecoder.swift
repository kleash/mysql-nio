import NIOCore
import Logging

public struct MySQLPacketDecoder: ByteToMessageDecoder {
    public typealias InboundOut = MySQLPacket

    public let sequence: MySQLPacketSequence
    let logger: Logger
    /// The 0xFFFFFF-byte packets so far of a payload that doesn't fit in one; a shorter packet ends it.
    // shortcut: a joined payload has no size limit (one packet used to stop at 16 MiB); cap it if servers can't be trusted with memory.
    var fragments: ByteBuffer?
    
    public init(sequence: MySQLPacketSequence, logger: Logger) {
        self.sequence = sequence
        self.logger = logger
    }
    
    public mutating func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        var copy = buffer
        guard let header = copy.readInteger(endianness: .little, as: UInt32.self) else {
            return .needMoreData
        }
        // header = 01     00 00 01
        //          <seq>  <length>
        let length = header & 0x00_FF_FF_FF
        let sequenceID = header >> 24
        // print("header: \(header) length: \(length) seq: \(sequenceID)")
        guard var payload = copy.readSlice(length: numericCast(length)) else {
            return .needMoreData
        }
        buffer = copy
        self.sequence.current = UInt8(sequenceID & 0xFF)
        if var fragments = self.fragments.take() {
            fragments.writeBuffer(&payload)
            payload = fragments
        }
        guard length < 0xFF_FF_FF else {
            self.fragments = payload
            return .continue
        }
        let packet = MySQLPacket(payload: payload)
        self.logger.trace("MySQLPacketDecoder.decode: \(packet)")
        context.fireChannelRead(self.wrapInboundOut(packet))
        return .continue
    }
    
    public mutating func decodeLast(context: ChannelHandlerContext, buffer: inout ByteBuffer, seenEOF: Bool) throws -> DecodingState {
        .needMoreData
    }
}
