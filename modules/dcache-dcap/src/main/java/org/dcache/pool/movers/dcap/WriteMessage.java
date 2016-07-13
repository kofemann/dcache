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

import diskCacheV111.vehicles.DCapProtocolInfo;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.dcache.pool.movers.DcapChannelMessage;
import org.dcache.pool.movers.NettyTransferService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class WriteMessage extends DcapChannelMessage {

    private static final Logger LOGGER = LoggerFactory.getLogger(WriteMessage.class);

    private final ByteBuf buf;
    private final ChannelHandlerContext ctx;


    private final NettyTransferService<DCapProtocolInfo>.NettyMoverChannel moverChannel;

    public WriteMessage(ChannelHandlerContext ctx, NettyTransferService<DCapProtocolInfo>.NettyMoverChannel moverChannel,
            ByteBuf buf) {

        // bump ref count to prevent buffer recycling by netty memory management
        buf.retain();

        this.ctx = ctx;
        this.buf = buf;
        this.moverChannel = moverChannel;
    }

    @Override
    public void process() throws IOException {

        try {
            ByteBuffer bb = buf.nioBuffer(buf.readerIndex(), buf.capacity());
            bb.limit(bb.capacity());
            while (bb.hasRemaining()) {
                moverChannel.write(bb);
            }
        } finally {
            // inform memory manager that buffer can be recycled
            buf.release();
        }
    }

}
