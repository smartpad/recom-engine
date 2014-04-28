package com.jinnova.smartpad.recom;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;

import com.jinnova.smartpad.db.ScriptRunner;
import com.jinnova.smartpad.partner.Catalog;
import com.jinnova.smartpad.partner.IDetailManager;
import com.jinnova.smartpad.partner.PartnerManager;
import com.jinnova.smartpad.partner.SmartpadCommon;
import com.jinnova.smartpad.partner.SystemCatalogGenrator;

public class ClientSupport {
	
	private final String drillDbhost;
	private final String drillDbport;
	private final String drillDbname;
	private final String drillDblogin;
	private final String drillDbpass;
	
	public static void main(String[] args) throws SQLException, FileNotFoundException, IOException {
		
		ClientSupport cs = new ClientSupport("localhost", null, "smartpad_drill", "root", "");
		cs.buildDrillDatabase("smartpad");
	}
	
	public void buildDrillDatabase(String mainDbname) throws SQLException, FileNotFoundException, IOException {
		//initialize
		dropDrillDatabaseIfExists();
		ScriptRunner.createDatabase(drillDbhost, drillDbport, drillDbname, drillDblogin, drillDbpass, true);
		SmartpadCommon.initialize(drillDbhost, drillDbport, drillDbname, drillDblogin, drillDbpass);
		new SystemCatalogGenrator(true).generate();
		PartnerManager.loadSyscatsInitially();
		copyNonClusterDataToDrilling(mainDbname);
	}

	public ClientSupport(String drillDbhost, String drillDbport,
			String drillDbname, String drillDblogin, String drillDbpass) {
		super();
		this.drillDbhost = drillDbhost;
		this.drillDbport = drillDbport;
		this.drillDbname = drillDbname;
		this.drillDblogin = drillDblogin;
		this.drillDbpass = drillDbpass;
	}
	
	Connection openConnection() throws SQLException {
		return DriverManager.getConnection(makeDburl(drillDbhost, drillDbport, drillDbname), drillDblogin, drillDbpass);
	}

	private void copyNonClusterDataToDrilling(String mainDbname) throws SQLException {

		Connection conn = DriverManager.getConnection(makeDburl(drillDbhost, drillDbport, drillDbname), drillDblogin, drillDbpass);
		Statement stmt = conn.createStatement();
		String sql = "delete from catalogs";
		System.out.println("SQL: " + sql);
		stmt.executeUpdate(sql);
		String[] tables = new String[] {"consumers", "catalogs", "operations", "promos"};
		for (String oneTable : tables) {
			sql = "insert into " + /*drillDbname + "." +*/ oneTable + " (select * from " + mainDbname + "." + oneTable + ")";
			System.out.println("SQL: " + sql);
			stmt.execute(sql);
		}
		
		LinkedList<Catalog> catList = new LinkedList<>();
		Catalog rootCat = PartnerManager.instance.getSystemRootCatalog();
		catList.addAll(PartnerManager.instance.getSystemSubCatalog(rootCat.getId()));
		while (!catList.isEmpty()) {
			Catalog cat = catList.removeFirst();
			sql = "create table " + cat.getId() + "_clusters";
			sql = "insert into " + cat.getId() + " (select * from " + mainDbname + "." + cat.getId() + ")";
			System.out.println("SQL: " + sql);
			stmt.execute(sql);
			LinkedList<Catalog> subCats = PartnerManager.instance.getSystemSubCatalog(cat.getId());
			if (subCats != null) {
				catList.addAll(subCats);
			}
		}
		
		stmt.close();
		conn.close();
	}
	
	private void dropDrillDatabaseIfExists() throws SQLException {

		Connection conn = DriverManager.getConnection(makeDburl(drillDbhost, drillDbport, "mysql"), drillDblogin, drillDbpass);
		Statement stmt = conn.createStatement();
		String sql = "drop database if exists " + drillDbname;
		System.out.println("SQL: " + sql);
		stmt.executeUpdate(sql);
		stmt.close();
		conn.close();
	}
	
	private static String makeDburl(String dbhost, String dbport, String dbname) {
		String dburl = "jdbc:mysql://" + dbhost;
		if (dbport != null) {
			dburl = dburl + ":" + dbport;
		}
		return dburl + "/" + dbname + "?useUnicode=true&characterEncoding=UTF-8";
	}
	
	static LinkedList<String> buildSyscatIdList() {
		LinkedList<String> allSyscatIds = new LinkedList<String>();
		LinkedList<Catalog> catList = new LinkedList<Catalog>();
		catList.addAll(PartnerManager.instance.getSystemSubCatalog(IDetailManager.SYSTEM_BRANCH_ID));
		while (!catList.isEmpty()) {
			Catalog cat = catList.removeFirst();
			allSyscatIds.add(cat.getId());
			LinkedList<Catalog> subCats = PartnerManager.instance.getSystemSubCatalog(cat.getId());
			if (subCats != null) {
				catList.addAll(subCats);
			}
		}
		return allSyscatIds;
	}
}
