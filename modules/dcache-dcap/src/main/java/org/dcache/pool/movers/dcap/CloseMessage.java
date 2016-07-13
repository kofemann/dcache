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
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import org.dcache.pool.movers.DCapConstants;
import org.dcache.pool.movers.DCapOutputByteBuffer;
import org.dcache.pool.movers.DcapChannelMessage;
import org.dcache.pool.movers.NettyTransferService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class CloseMessage extends DcapChannelMessage {

    private static final Logger LOGGER = LoggerFactory.getLogger(CloseMessage.class);

    private final ChannelHandlerContext ctx;
    private final DCapOutputByteBuffer cntOut;

    private final NettyTransferService<DCapProtocolInfo>.NettyMoverChannel moverChannel;

    public CloseMessage(ChannelHandlerContext ctx, NettyTransferService<DCapProtocolInfo>.NettyMoverChannel moverChannel,
            DCapOutputByteBuffer cntOut) {

        this.ctx = ctx;
        this.cntOut = cntOut;
        this.moverChannel = moverChannel;
    }

    @Override
    public void process() {
        moverChannel.release();

        cntOut.writeACK(DCapConstants.IOCMD_CLOSE);
        ctx.writeAndFlush(Unpooled.copiedBuffer(cntOut.buffer()));
        ctx.channel().close();
    }

}
