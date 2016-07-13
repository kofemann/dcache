/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2000-2016 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.pool.movers;

import diskCacheV111.vehicles.DCapProtocolInfo;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.nio.ByteOrder;
import java.util.List;
import org.dcache.pool.movers.dcap.CloseMessage;
import org.dcache.pool.movers.dcap.ReadMessage;
import org.dcache.pool.movers.dcap.WriteMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.dcache.util.ByteUnit.KiB;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;

/**
 * A {@link ByteToMessageDecoder} which reads decodes stream of bytes into DCAP
 * control channel messages.
 */
public class DcapDecoder extends ByteToMessageDecoder {

    private enum DecoderState {
        IDLE,
        WATING_FOR_DATA_BLOCK,
        WATING_FOR_DATA_BLOCK_BEGIN,
        WATING_FOR_DATA,
    };

    private static final Logger LOGGER = LoggerFactory.getLogger(DcapDecoder.class);
    private static final int MIN_REQUEST_SIZE = 8; // 4 command size + 4 command code

    private final DCapOutputByteBuffer cntOut = new DCapOutputByteBuffer(KiB.toBytes(1));
    private final NettyTransferService<DCapProtocolInfo>.NettyMoverChannel moverChannel;

    private DecoderState decoderState = DecoderState.IDLE;

    private int bytesToReceive;

    public DcapDecoder(NettyTransferService<DCapProtocolInfo>.NettyMoverChannel moverChannel) {
        this.moverChannel = moverChannel;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf bb, List<Object> out) throws Exception {

        bb.order(ByteOrder.BIG_ENDIAN);

        switch(decoderState) {
            case IDLE: {
                bb.markReaderIndex();
                int readableBytes = bb.readableBytes();

                if (readableBytes < MIN_REQUEST_SIZE) {
                    // short read
                    LOGGER.warn("Short read");
                    return;
                }

                int commandSize = bb.readInt();
                int commandCode = bb.readInt();

                switch (commandCode) {

                    case DCapConstants.IOCMD_WRITE:

                        cntOut.writeACK(DCapConstants.IOCMD_WRITE);
                        ctx.writeAndFlush(Unpooled.copiedBuffer(cntOut.buffer()));
                        decoderState = DecoderState.WATING_FOR_DATA_BLOCK_BEGIN;
                        return;

                    case DCapConstants.IOCMD_READ:
                        out.add(new ReadMessage(ctx, moverChannel, bb.readLong(), cntOut));
                        break;
                    case DCapConstants.IOCMD_SEEK:
                    case DCapConstants.IOCMD_SEEK_AND_READ:
                    case DCapConstants.IOCMD_SEEK_AND_WRITE:
                    case DCapConstants.IOCMD_CLOSE:
                        if (commandSize - 4 > bb.readableBytes()) { // as we already have read command code
                            // the checksum block is missing, wait for more data
                            bb.resetReaderIndex();
                            return;
                        }
                        if (commandSize > 8) {
                            int checksumBlockSize = bb.readInt();
                            bb.readInt(); // checksum indicator
                            int checkSumType = bb.readInt();
                            int checksumSize = checksumBlockSize - 8;
                            byte[] checksum = new byte[checksumSize];
                            bb.readBytes(checksum);

                            try {
                                ChecksumType cs = ChecksumType.getChecksumType(checkSumType);
                                LOGGER.warn("checksum type: {}, size: {}", checkSumType, checksumSize);
                                // FIXME: push it down to mover
                            } catch (IllegalArgumentException e) {
                                LOGGER.warn("Unsupported checksum type: {}", checkSumType);
                            }
                        }
                        out.add(new CloseMessage(ctx, moverChannel, cntOut));
                        break;
                    case DCapConstants.IOCMD_LOCATE:
                    case DCapConstants.IOCMD_READV:
                    default:
                        LOGGER.warn("Unsuported command {} from client: {}", commandCode, ctx.channel().remoteAddress());
                        return;
                }
            }
            break;
            case WATING_FOR_DATA_BLOCK_BEGIN:
            {
                int readableBytes = bb.readableBytes();
                if (readableBytes < 8) {
                    return;
                }

                // TODO: add some checks
                bb.readInt(); // bytes follow (4)
                bb.readInt(); // whats is commitg (DATA)

                decoderState = DecoderState.WATING_FOR_DATA_BLOCK;
            }
            break;
            case WATING_FOR_DATA_BLOCK:
            {
                if (bb.readableBytes() < 4) {
                    // not enough data even to get size of the next block
                    bb.resetReaderIndex();
                    return;
                }

                bytesToReceive = bb.readInt();
                if (bytesToReceive < 0) {
                    // end of write
                    cntOut.writeFIN(DCapConstants.IOCMD_WRITE);
                    ctx.writeAndFlush(Unpooled.copiedBuffer(cntOut.buffer()));
                    decoderState = DecoderState.IDLE;
                    return;
                }

                if (bytesToReceive == 0) {
                    // wait for next data block
                    return;
                }

                LOGGER.debug("bytes in data block: {}", bytesToReceive);
                decoderState = DecoderState.WATING_FOR_DATA;
            }
            break;
            case WATING_FOR_DATA:
            {
                int dataAvailable = bb.readableBytes();
                int bytesInPipe = Math.min(dataAvailable, bytesToReceive);
                LOGGER.debug("bytes in pipeline: {}, to receive: {}", bytesInPipe, bytesToReceive);
                out.add(new WriteMessage(ctx, moverChannel, bb.readSlice(bytesInPipe)));
                bytesToReceive -= bytesInPipe;
                if (bytesToReceive == 0) {
                    decoderState = DecoderState.WATING_FOR_DATA_BLOCK;
                }
            }
            break;
            default:
                throw new RuntimeException();
        }

    }

}
