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
package org.dcache.pool.movers.dcap;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.DCapProtocolInfo;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.dcache.pool.movers.DCapConstants;
import org.dcache.pool.movers.DCapOutputByteBuffer;
import org.dcache.pool.movers.DcapChannelMessage;
import org.dcache.pool.movers.NettyTransferService;
import org.dcache.util.ByteUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadMessage extends DcapChannelMessage {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadMessage.class);

    private static final int MAX_CHUNK_SIZE = ByteUnit.KiB.toBytes(1024);

    private long bytesToSend;
    private final ChannelHandlerContext ctx;
    private final DCapOutputByteBuffer cntOut;

    private final NettyTransferService<DCapProtocolInfo>.NettyMoverChannel moverChannel;

    public ReadMessage(ChannelHandlerContext ctx, NettyTransferService<DCapProtocolInfo>.NettyMoverChannel moverChannel,
            long bytes, DCapOutputByteBuffer cntOut) {

        this.ctx = ctx;
        this.bytesToSend = bytes;
        this.cntOut = cntOut;
        this.moverChannel = moverChannel;
    }

    @Override
    public void process() {

        ByteBuf chunk = null;
        try {

            cntOut.writeACK(DCapConstants.IOCMD_READ);
            ctx.write(Unpooled.copiedBuffer(cntOut.buffer()));

            cntOut.writeDATA_HEADER();
            ctx.write(Unpooled.copiedBuffer(cntOut.buffer()));

            while(bytesToSend > 0) {

                chunk = ctx.alloc().buffer((int)Math.min(MAX_CHUNK_SIZE, bytesToSend) + 4); // + stace for size
                ByteBuffer bb = chunk.nioBuffer(0, chunk.capacity());

                bb.limit(bb.capacity());
                bb.position(4);
                LOGGER.debug("Rerading {} bytes ({}, {}).", bb.limit(), bytesToSend, bb.capacity());
                int n = moverChannel.read(bb);

                if (n == -1) {
                    bb.putInt(0, 0);
                    break;
                } else {
                    bb.putInt(0, n);
                }

                chunk.writerIndex(bb.position());
                ctx.write(chunk);
                bytesToSend -= n;

                LOGGER.debug("{} bytes sent", n);
            }

            cntOut.writeDATA_TRAILER();
            ctx.write(Unpooled.copiedBuffer(cntOut.buffer()));

            cntOut.writeFIN(DCapConstants.IOCMD_READ);
            ctx.writeAndFlush(Unpooled.copiedBuffer(cntOut.buffer()));

        } catch (IOException e) {

            if(chunk != null) {
                chunk.release();
            }

            cntOut.writeFIN(DCapConstants.IOCMD_READ,CacheException.ERROR_IO_DISK,e.getMessage());
            ctx.writeAndFlush(Unpooled.copiedBuffer(cntOut.buffer()));
        }

    }
}
