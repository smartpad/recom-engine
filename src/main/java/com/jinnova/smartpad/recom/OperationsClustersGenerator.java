package com.jinnova.smartpad.recom;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;

import com.jinnova.smartpad.db.DbIterator;

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
		DbIterator<String> clusters = cs.iterateClusters();
		LinkedList<String> syscats = ClientSupport.buildSyscatIdList();
		while (clusters.hasNext()) {
			String cluid = clusters.next();
			for (String oneSyscat : syscats) {
				String sql = "insert into operations_clusters (select " + cluid + "," + 1 + ", operations.* " +
						"from operations where syscat_id='" + oneSyscat + "' limit 50);";
				System.out.println("SQL: " + sql);
				stmt.executeUpdate(sql);
			}
		}
		clusters.close();
		stmt.close();
		conn.close();
	}

}
