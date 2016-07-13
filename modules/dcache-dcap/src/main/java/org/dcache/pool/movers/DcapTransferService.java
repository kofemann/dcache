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

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dmg.cells.nucleus.CellPath;

import diskCacheV111.util.DCapProrocolChallenge;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.PoolPassiveIoFileMessage;

import org.dcache.util.NetworkUtils;
/**
 *
 */
public class DcapTransferService extends NettyTransferService<DCapProtocolInfo> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DcapTransferService.class);

    public DcapTransferService() {
        super("dcap");
    }

    @Override
    protected void sendAddressToDoor(NettyMover<DCapProtocolInfo> mover, int port) throws Exception {

        DCapProtocolInfo protocolInfo = mover.getProtocolInfo();

        byte[] uuid = mover.getUuid().toString().getBytes(StandardCharsets.US_ASCII);

        DCapProrocolChallenge challenge = new DCapProrocolChallenge(protocolInfo.getSessionId(), uuid);

        InetAddress localIP
                = NetworkUtils.getLocalAddress(protocolInfo.getSocketAddress().getAddress());

        InetSocketAddress socketAddress = new InetSocketAddress(localIP, port);
        PoolPassiveIoFileMessage<byte[]> moverAddressMessage = new PoolPassiveIoFileMessage<>("pool",  socketAddress, uuid);
        moverAddressMessage.setId(protocolInfo.getSessionId());

        CellPath dcapDoor = new CellPath(mover.getPathToDoor().getDestinationAddress());
        doorStub.notify(dcapDoor, moverAddressMessage);
    }

    @Override
    protected UUID createUuid(DCapProtocolInfo protocolInfo) {
        return UUID.randomUUID();
    }

    @Override
    protected void initChannel(Channel ch) throws Exception {
        super.initChannel(ch);

        ChannelPipeline pipeline = ch.pipeline();

        pipeline.addLast("handshake", new DcapDataChannelHandshakeHendler(this));
    }
}
