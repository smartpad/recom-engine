package com.jinnova.smartpad.recom;

import static com.jinnova.smartpad.recom.ClientSupport.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.LinkedList;

import com.jinnova.smartpad.db.DbIterator;
import com.jinnova.smartpad.db.PromotionDao;
import com.jinnova.smartpad.member.CCardBranch;
import com.jinnova.smartpad.member.CCardRequirement;
import com.jinnova.smartpad.member.CCardType;
import com.jinnova.smartpad.partner.Promotion;
import com.jinnova.smartpad.partner.Schedule;

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
		DbIterator<String> clusters = cs.iterateClusters();

		//for each cluster, generate syscat/promotion pairs
		while (clusters.hasNext()) {
			String cluid = clusters.next();
			
			//for each syscat, generate at most 100 promotions
			int count = 0;
			Date now = new Date();
			for (String oneSyscatId : allSyscatIds) {
				DbIterator<Promotion> promos = new PromotionDao().iteratePromosBySyscat(oneSyscatId, null, RECURSIVE, null);
				while (count < 50 && promos.hasNext()) {
					Promotion p = promos.next();
					Schedule schedule = p.getSchedule();
					if (schedule != null) {
						Date latestEnd = schedule.getLatestEnd();
						if (latestEnd != null && latestEnd.before(now)) {
							continue;
						}
					}
					count++;
					CCardRequirement visaCredit = p.getCCardOpt(CCardBranch.visa, CCardType.credit);
					CCardRequirement visaDebit = p.getCCardOpt(CCardBranch.visa, CCardType.debit);
					CCardRequirement masterCredit = p.getCCardOpt(CCardBranch.master, CCardType.credit);
					CCardRequirement masterDebit = p.getCCardOpt(CCardBranch.master, CCardType.debit);
					String sql = "insert into promos_clusters ("
							+ "cluster_id, cluster_rank, syscat_id, "
							+ "visa_c, visa_c_issuers, visa_d, visa_d_issuers, "
							+ "master_c, master_c_issuers, master_d, master_d_issuers, "
							+ "promo_id, branch_id, store_id, gps_lon, gps_lat, "
							+ "name, descript, images, create_date, create_by"
							+ ") "
							
							+ "(select "
							+ cluid + ", 1, '" + oneSyscatId + "', "
							+ (visaCredit == null ? 0 : 1) + ", " + (visaCredit == null ? "null" : visaCredit.requiredCCardIssuer) + ", "
							+ (visaDebit == null ? 0 : 1) + ", " + (visaDebit == null ? "null" : visaDebit.requiredCCardIssuer) + ", " 
							+ (masterCredit == null ? 0 : 1) + ", "
							+ (masterCredit == null ? "null" : masterCredit.requiredCCardIssuer) + ", " 
							+ (masterDebit == null ? 0 : 1) + ", " + (masterDebit == null ? "null" : masterDebit.requiredCCardIssuer) + ", "
							//+ p.getSchedule().getEarliestStart()
							+ "promo_id, branch_id, store_id, gps_lon, gps_lat, "
							+ "name, descript, images, create_date, create_by "
							
							+ "from promos where promo_id='" + p.getId() + "')";
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
