package com.jinnova.smartpad.recom;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;

import com.jinnova.smartpad.db.DbIterator;
import com.jinnova.smartpad.db.PromotionDao;
import com.jinnova.smartpad.member.CCardBranch;
import com.jinnova.smartpad.member.CCardRequirement;
import com.jinnova.smartpad.member.CCardType;
import com.jinnova.smartpad.partner.Promotion;

public class PromotionsClustersGenerator {
	
	private ClientSupport cs;
	
	public static void main(String[] args) throws SQLException, FileNotFoundException, IOException {
		
		ClientSupport cs = new ClientSupport("localhost", null, "smartpad_drill", "root", "");
		new PromotionsClustersGenerator(cs).generate();
	}
	
	PromotionsClustersGenerator(ClientSupport cs) {
		this.cs = cs;
	}

	void generate() throws SQLException { 
		
		Connection conn = cs.openConnection();
		Statement stmt = conn.createStatement();
		LinkedList<String> allSyscatIds = ClientSupport.buildSyscatIdList();

		//for each cluster, generate syscat/promotion pairs
		for (int i = 1; i <= 3; i++) {
			
			//for each syscat, generate at most 100 promotions
			for (String oneSyscatId : allSyscatIds) {
				DbIterator<Promotion> promos = new PromotionDao().iterateSyscatPromos(oneSyscatId);
				while (promos.hasNext()) {
					Promotion p = promos.next();
					CCardRequirement visaCredit = p.getCCardOpt(CCardBranch.visa, CCardType.credit);
					CCardRequirement visaDebit = p.getCCardOpt(CCardBranch.visa, CCardType.debit);
					CCardRequirement masterCredit = p.getCCardOpt(CCardBranch.master, CCardType.credit);
					CCardRequirement masterDebit = p.getCCardOpt(CCardBranch.master, CCardType.debit);
					String sql = "insert into promos_clusters ("
							+ "cluster_id, cluster_rank, "
							+ "visa_c, visa_c_issuer, visa_d, visa_d_issuer, "
							+ "master_c, master_c_issuer, master_d, master_d_issuer, "
							+ "promo_id, branch_id, store_id, syscat_id, gps_lon=, gps_lat, "
							+ "name, descript, images"
							+ ") "
							
							+ "(select "
							+ "1, 1, "
							+ (visaCredit == null ? 0 : 1) + ", " + (visaCredit == null ? "null" : visaCredit.requiredCCardIssuer) + ", "
							+ (visaDebit == null ? 0 : 1) + ", " + (visaDebit == null ? "null" : visaDebit.requiredCCardIssuer) + ", " 
							+ (masterCredit == null ? 0 : 1) + ", "
							+ (masterCredit == null ? "null" : masterCredit.requiredCCardIssuer) + ", " 
							+ (masterDebit == null ? 0 : 1) + ", " + (masterDebit == null ? "null" : masterDebit.requiredCCardIssuer) + ", "
							+ p.getSchedule().getEarliestStart()
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
