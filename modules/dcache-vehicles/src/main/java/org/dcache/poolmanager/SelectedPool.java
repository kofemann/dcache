/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
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

package org.dcache.poolmanager;

import dmg.cells.nucleus.CellAddressCore;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Result of a pool selection.
 *
 */
public class SelectedPool
{
    private final PoolInfo info;

    public SelectedPool(PoolInfo info)
    {
        this.info = checkNotNull(info);
    }

    public PoolInfo info()
    {
        return info;
    }

    public String name()
    {
        return info.getName();
    }

    public String hostName()
    {
        return info.getHostName();
    }

    public CellAddressCore address()
    {
        return info.getAddress();
    }
}
