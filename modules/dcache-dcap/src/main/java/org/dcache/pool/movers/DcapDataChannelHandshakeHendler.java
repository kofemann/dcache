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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A ChannelHandler which recognizes DCAP client connection and matches it to a
 * corresponding mover.
 */
public class DcapDataChannelHandshakeHendler extends ByteToMessageDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(DcapDataChannelHandshakeHendler.class);

    /**
     * The server on which this request handler is running.
     */
    private final DcapTransferService dcapService;

    DcapDataChannelHandshakeHendler(DcapTransferService dcapService) {
        this.dcapService = dcapService;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf bb, List<Object> list) throws Exception {

        int sessionId = bb.readInt();
        int challengeLen = bb.readInt();

        byte[] chalangeBytesBase64 = new byte[challengeLen];
        bb.readBytes(chalangeBytesBase64);

        byte[] chalangeBytes = Base64.getDecoder().decode(chalangeBytesBase64);


        UUID uuid = UUID.fromString(new String(chalangeBytes, StandardCharsets.US_ASCII));
        NettyTransferService<DCapProtocolInfo>.NettyMoverChannel moverChannel =  dcapService.openFile(uuid, true);

        ChannelPipeline pipeline = ctx.channel().pipeline();

        pipeline.addLast("dcap-decoder", new DcapDecoder(moverChannel));
        pipeline.addLast("dcap-transfer", new DcapRequestHandler(dcapService));
        pipeline.remove(this);
    }

}
