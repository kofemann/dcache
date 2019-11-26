/*
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */
package org.dcache.chimera;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.IOException;
import java.sql.SQLException;

import org.dcache.db.AlarmEnabledDataSource;

public class FsFactory
{
    public static FileSystemProvider createFileSystem(String url, String user, String password) throws SQLException, ChimeraFsException
    {
        AlarmEnabledDataSource dataSource = new AlarmEnabledDataSource(url, FsFactory.class.getSimpleName(), getDataSource(url, user, password));
        PlatformTransactionManager txManager =  new DataSourceTransactionManager(dataSource);
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        return new JdbcFs(dataSource, txManager, hz) {
            @Override
            public void close() throws IOException
            {
                super.close();
                dataSource.close();
                hz.shutdown();
            }
        };
    }

    public static HikariDataSource getDataSource(String url, String user, String pass)
    {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(pass);
        config.setMaximumPoolSize(3);
        config.setMinimumIdle(0);
        return new HikariDataSource(config);
    }
}
