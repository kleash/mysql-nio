import Logging
import NIOCore

public final class MySQLPacketEncoder: MessageToByteEncoder {
    public typealias OutboundIn = MySQLPacket
    
    public let sequence: MySQLPacketSequence
    let logger: Logger

    public init(sequence: MySQLPacketSequence, logger: Logger) {
        self.sequence = sequence
        self.logger = logger
    }
    
    public func encode(data: MySQLPacket, out: inout ByteBuffer) throws {
        var packet = data
        self.logger.trace("MySQLPacketDecoder.encode: \(packet)")
        // A payload of 0xFFFFFF bytes or more goes out as 0xFFFFFF-byte packets and a last, shorter (maybe empty) one.
        var length: Int
        repeat {
            length = min(packet.payload.readableBytes, 0xFF_FF_FF)
            out.writeInteger(UInt8(length & 0xFF))
            out.writeInteger(UInt8(length >> 8 & 0xFF))
            out.writeInteger(UInt8(length >> 16 & 0xFF))
            out.writeInteger(UInt8(self.sequence.next()))
            out.writeImmutableBuffer(packet.payload.readSlice(length: length)!)
        } while length == 0xFF_FF_FF
    }
}
