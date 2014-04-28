package com.jinnova.smartpad.recom;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class OperationsClustersGenerator {
	
	private ClientSupport cs;
	
	public static void main(String[] args) throws SQLException, FileNotFoundException, IOException {
		
		ClientSupport cs = new ClientSupport("localhost", null, "smartpad_drill", "root", "");
		new OperationsClustersGenerator(cs).generate();
	}
	
	OperationsClustersGenerator(ClientSupport cs) {
		this.cs = cs;
	}

	void generate() throws SQLException { 

		Connection conn = cs.openConnection();
		Statement stmt = conn.createStatement();
		for (int i = 1; i <= 3; i++) {
			String sql = "insert into clusters values (" + i + ")";
			System.out.println("SQL: " + sql);
			stmt.executeUpdate(sql);
			sql = "insert into operations_clusters (select " + i + "," + i + ", operations.* from operations);";
			System.out.println("SQL: " + sql);
			stmt.executeUpdate(sql);
		}
		stmt.close();
		conn.close();
	}

}
