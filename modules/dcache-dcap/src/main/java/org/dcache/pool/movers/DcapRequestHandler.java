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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class DcapRequestHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(DcapRequestHandler.class);

    /**
     * The server on which this request handler is running.
     */
    private final NettyTransferService<DCapProtocolInfo> server;

    public DcapRequestHandler(NettyTransferService<DCapProtocolInfo> server) {
        this.server = server;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
       if (msg instanceof DcapChannelMessage) {
           LOGGER.debug("Processing request: {}", msg.getClass().getName());
           ((DcapChannelMessage)msg).process();
       }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause); //To change body of generated methods, choose Tools | Templates.
    }

}
