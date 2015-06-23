/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 * <p/>
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 */


package data.sync.common;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;


public class DBUtils {
    private DBUtils() {
    }

    public static ResultSet query(Connection conn, String sql) throws SQLException {
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        return query(stmt, sql);
    }


    public static ResultSet query(Statement stmt, String sql) throws SQLException {
        return stmt.executeQuery(sql);
    }

    public static void closeResultSet(ResultSet rs) {
        try {
            if (null != rs) {
                Statement stmt = rs.getStatement();
                if (null != stmt) {
                    stmt.close();
                    stmt = null;
                }
                rs.close();
            }
            rs = null;
        } catch (SQLException e) {
            throw new IllegalStateException(e.getCause());
        }
    }


	public static MetaData genMetaData(Connection conn, String sql)
			throws SQLException {
		MetaData meta = new MetaData();
		List<MetaData.Column> columns = new ArrayList<MetaData.Column>();

		ResultSet resultSet = null;
		try {
			resultSet = query(conn, sql);
			int columnCount = resultSet.getMetaData().getColumnCount();
			for (int i = 1; i <= columnCount; i++) {
				MetaData.Column col = meta.new Column();
				col.setColName(resultSet.getMetaData().getColumnName(i)
						.toLowerCase().trim());
				col.setDataType(resultSet.getMetaData().getColumnTypeName(i)
						.toLowerCase().trim());
				columns.add(col);
			}
			meta.setColInfo(columns);
			meta.setTableName(resultSet.getMetaData().getTableName(1).toLowerCase());
		} finally {
			closeResultSet(resultSet);
		}

		return meta;
	}

}