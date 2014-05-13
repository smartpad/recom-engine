package com.jinnova.smartpad.recom;

import static com.jinnova.smartpad.partner.ICatalogField.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;

import com.jinnova.smartpad.db.CatalogDao;
import com.jinnova.smartpad.partner.Catalog;
import com.jinnova.smartpad.partner.CatalogField;
import com.jinnova.smartpad.partner.CatalogSpec;
import com.jinnova.smartpad.partner.ICatalogField;
import com.jinnova.smartpad.partner.IDetailManager;
import com.jinnova.smartpad.partner.PartnerManager;

public class CatGroupingGenerator {
	
	private ClientSupport cs;
	
	public static void main(String[] args) throws SQLException, FileNotFoundException, IOException {
		
		/*ClientSupport cs = new ClientSupport("localhost", null, "smartpad_drill", "root", "");
		new CatGroupingGenerator(cs).generate();*/
		System.out.print(new BigInteger("0709C204664A15F57F12154F05C5A397", 16));
	}
	
	CatGroupingGenerator(ClientSupport cs) {
		this.cs = cs;
	}

	void generate() throws SQLException {

		Connection conn = cs.openConnection();
		Statement stmt = conn.createStatement();
		LinkedList<Catalog> catList = new LinkedList<>();
		catList.addAll(PartnerManager.instance.getSystemSubCatalog(IDetailManager.SYSTEM_BRANCH_ID));
		while (!catList.isEmpty()) {
			Catalog cat = catList.removeFirst();
			CatalogSpec spec = (CatalogSpec) cat.getCatalogSpec();
			boolean modified = false;
			for (ICatalogField ifield : spec.getAllFields()) {
				CatalogField field = (CatalogField) ifield;
				if (field.getGroupingType() == SEGMENT_DISTINCT) {
					String fid = field.getId() + ICatalogField.SEGMENT_POSTFIX;
					String sql = "select " + fid + ", " + field.getId() + 
							" from " + spec.getSpecId() + " where syscat_id like '" + cat.getId() + "%' group by " + fid;
					System.out.println("SQL: " + sql);
					ResultSet rs = stmt.executeQuery(sql);
					//JsonArray ja = new JsonArray();
					while (rs.next()) {
						/*JsonObject json = new JsonObject();
						json.add(CatalogField.ATT_GROUPING_VALUEID, new JsonPrimitive(rs.getString(1)));
						json.add(CatalogField.ATT_GROUPING_VALUE, new JsonPrimitive(rs.getString(2)));
						ja.add(json);*/
						cat.addSegment(field.getId(), rs.getString(1), rs.getString(2));
						modified = true;
					}
					//field.setAttribute(CatalogField.ATT_GROUPING, ja);
				}
			}
			
			if (modified) {
				//new CatalogDao().updateSpec(cat.getId(), cat);
				new CatalogDao().updateSegments(cat);
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
