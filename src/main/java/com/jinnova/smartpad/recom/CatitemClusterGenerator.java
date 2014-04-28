package com.jinnova.smartpad.recom;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;

import com.jinnova.smartpad.db.DbIterator;
import com.jinnova.smartpad.partner.Catalog;
import com.jinnova.smartpad.partner.IDetailManager;
import com.jinnova.smartpad.partner.PartnerManager;

public class CatitemClusterGenerator {
	
	private ClientSupport cs;
	
	public static void main(String[] args) throws SQLException, FileNotFoundException, IOException {
		
		ClientSupport cs = new ClientSupport("localhost", null, "smartpad_drill", "root", "");
		new CatitemClusterGenerator(cs).generate();
	}
	
	CatitemClusterGenerator(ClientSupport cs) {
		this.cs = cs;
	}

	void generate() throws SQLException {

		Connection conn = cs.openConnection();
		Statement stmt = conn.createStatement();
		LinkedList<Catalog> catList = new LinkedList<>();
		catList.addAll(PartnerManager.instance.getSystemSubCatalog(IDetailManager.SYSTEM_BRANCH_ID));
		while (!catList.isEmpty()) {
			Catalog cat = catList.removeFirst();
			DbIterator<String> clusters = cs.iterateClusters();
			while (clusters.hasNext()) {
				String clu = clusters.next();
				String sql = "insert into " + cat.getId() + "_c (select " + clu + ", 1, " + cat.getId() + ".* from " + cat.getId() + 
						" where syscat_id like '" + cat.getId() + "%' limit 50)";
				System.out.println("SQL: " + sql);
				stmt.execute(sql);
			}
			LinkedList<Catalog> subCats = PartnerManager.instance.getSystemSubCatalog(cat.getId());
			if (subCats != null) {
				catList.addAll(subCats);
			}
		}
		stmt.close();
		conn.close();
		
	}
}
