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

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.support.JdbcUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DirectoryStreamImpl
{
    private static final String QUERY =
            "SELECT i.*, d.iname d.icookie FROM t_inodes i JOIN t_dirs d ON i.inumber = d.ichild WHERE d.iparent=? " +
            "UNION ALL " +
            "SELECT i.*, '.' FROM t_inodes i WHERE i.inumber=? " +
            "UNION ALL " +
            "SELECT i.*, '..' FROM t_inodes i JOIN t_dirs d ON i.inumber = d.iparent WHERE d.ichild=?";

    private static final String QUERY2
            = "SELECT i.*, d.iname, d.icookie FROM t_inodes i JOIN t_dirs d ON i.inumber = d.ichild WHERE d.iparent=? "
            + "AND icookie > ? ORDER BY icookie LIMIT ?";

    private final ResultSet _resultSet;
    private final JdbcTemplate _jdbc;
    private final Connection _connection;
    private final PreparedStatement _statement;

    DirectoryStreamImpl(FsInode dir, JdbcTemplate jdbc)
    {
        _jdbc = jdbc;

        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs;
        try {
            connection = DataSourceUtils.getConnection(_jdbc.getDataSource());
            ps = connection.prepareStatement(QUERY);
            ps.setFetchSize(50);
            ps.setLong(1, dir.ino());
            ps.setLong(2, dir.ino());
            ps.setLong(3, dir.ino());
            rs = ps.executeQuery();
        } catch (SQLException ex) {
            JdbcUtils.closeStatement(ps);
            DataSourceUtils.releaseConnection(connection, _jdbc.getDataSource());
            throw _jdbc.getExceptionTranslator().translate("StatementExecution", QUERY, ex);
        }
        _connection = connection;
        _resultSet = rs;
        _statement = ps;
    }

    DirectoryStreamImpl(FsInode dir, JdbcTemplate jdbc, long cookie, int count) {
        _jdbc = jdbc;

        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs;
        try {
            connection = DataSourceUtils.getConnection(_jdbc.getDataSource());
            ps = connection.prepareStatement(QUERY2);
            ps.setFetchSize(50);
            ps.setLong(1, dir.ino());
            ps.setLong(2, cookie);
            ps.setInt(3, count);
            rs = ps.executeQuery();
        } catch (SQLException ex) {
            JdbcUtils.closeStatement(ps);
            DataSourceUtils.releaseConnection(connection, _jdbc.getDataSource());
            throw _jdbc.getExceptionTranslator().translate("StatementExecution", QUERY, ex);
        }
        _connection = connection;
        _resultSet = rs;
        _statement = ps;
    }

    public void close() throws IOException
    {
        try {
            JdbcUtils.closeResultSet(_resultSet);
            JdbcUtils.closeStatement(_statement);
            DataSourceUtils.releaseConnection(_connection, _jdbc.getDataSource());
        } catch (DataAccessException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    public ResultSet next() throws SQLException
    {
        return _resultSet.next() ? _resultSet : null;
    }
}
