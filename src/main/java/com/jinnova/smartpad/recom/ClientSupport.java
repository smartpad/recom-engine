package com.jinnova.smartpad.recom;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;

import com.jinnova.smartpad.db.DbIterator;
import com.jinnova.smartpad.db.PromotionDao;
import com.jinnova.smartpad.db.ScriptRunner;
import com.jinnova.smartpad.member.CCardBranch;
import com.jinnova.smartpad.member.CCardType;
import com.jinnova.smartpad.partner.Catalog;
import com.jinnova.smartpad.partner.IDetailManager;
import com.jinnova.smartpad.partner.PartnerManager;
import com.jinnova.smartpad.partner.Promotion;
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
		SystemCatalogGenrator.generate();
		PartnerManager.loadSyscatsInitially();
		copyDataToDrilling(mainDbname);
		generateClusterData();
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

	private void copyDataToDrilling(String mainDbname) throws SQLException {

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
			sql = "insert into " + cat.getId() + " (select * from " + mainDbname + "." + cat.getId() + ")";
			System.out.println("SQL: " + sql);
			stmt.execute(sql);
			LinkedList<Catalog> subCats = PartnerManager.instance.getSystemSubCatalog(cat.getId());
			if (subCats != null) {
				catList.addAll(subCats);
			}
		}
		
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

	private void generateClusterData() throws SQLException { 
		
		Connection conn = DriverManager.getConnection(makeDburl(drillDbhost, drillDbport, drillDbname), drillDblogin, drillDbpass);
		Statement stmt = conn.createStatement();
		for (int i = 1; i <= 3; i++) {
			String sql = "insert into clusters values (" + i + ")";
			System.out.println("SQL: " + sql);
			stmt.executeUpdate(sql);
			sql = "insert into operations_clusters (select " + i + "," + i + ", operations.* from operations);";
			System.out.println("SQL: " + sql);
			stmt.executeUpdate(sql);
		}
		
		/*
		 insert into promos_clusters (cluster_id, cluster_rank, promo_id, store_id, branch_id, syscat_id, 
			`name`,`descript`,`images`,`member_level`,`member_point`,
		  	`gps_lon`,`gps_lat`,`gps_inherit`,`create_date`,`update_date`,`create_by`,`update_by`)
		  	 
			(select 1, 1, promo_id, store_id, branch_id, syscat_id, 
			`name`,`descript`,`images`,`member_level`,`member_point`,
		  	`gps_lon`,`gps_lat`,`gps_inherit`,`create_date`,`update_date`,`create_by`,`update_by`
		  	from promos);
		 */
		
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

		//for each cluster, generate syscat/promotion pairs
		for (int i = 1; i <= 3; i++) {
			
			//for each syscat, generate at most 100 promotions
			for (String oneSyscatId : allSyscatIds) {
				DbIterator<Promotion> promos = new PromotionDao().iterateSyscatPromos(oneSyscatId);
				while (promos.hasNext()) {
					Promotion p = promos.next();
					int visaCredit = p.getCCardOpt(CCardBranch.visa, CCardType.credit) != null ? 1 : 0;
					int visaDebit = p.getCCardOpt(CCardBranch.visa, CCardType.debit) != null ? 1 : 0;
					int masterCredit = p.getCCardOpt(CCardBranch.master, CCardType.credit) != null ? 1 : 0;
					int masterDebit = p.getCCardOpt(CCardBranch.master, CCardType.debit) != null ? 1 : 0;
					String sql = "insert into promos_clusters ("
							+ "cluster_id, cluster_rank, "
							+ "visa_c, visa_c_issuer, visa_d, visa_d_issuer, "
							+ "master_c, master_c_issuer, master_d, master_d_issuer, "
							+ "promo_id, branch_id, store_id, syscat_id, gps_lon=, gps_lat, "
							+ "name, descript, images"
							+ ") "
							
							+ "(select "
							+ "1, 1, "
							+ visaCredit + ", null, " + visaDebit + ", null, " 
							+ masterCredit + ", null, " + masterDebit + ", null, "
							+ "promo_id, branch_id, store_id, syscat_id, gps_lon=, gps_lat, "
							+ "name, descript, images)"
							
							+ "where promo_id='" + p.getId() + "')";
					System.out.println("SQL: " + sql);
					stmt.executeUpdate(sql);
				}
				promos.close();
			}
		}
		stmt.close();
		conn.close();
	}
}
