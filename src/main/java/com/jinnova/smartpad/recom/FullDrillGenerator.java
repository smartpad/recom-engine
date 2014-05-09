package com.jinnova.smartpad.recom;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;

public class FullDrillGenerator {
	
	public static void main(String[] args) throws SQLException, FileNotFoundException, IOException {
		
		ClientSupport cs = new ClientSupport("localhost", null, "smartpad_drill", "root", "");
		cs.buildDrillDatabase("smartpad");
		cs.generateDummyClusters();
		new OperationsClustersGenerator(cs).generate();
		new PromotionsClustersGenerator(cs).generate();
		new CatitemClusterGenerator(cs).generate();
		new CatGroupingGenerator(cs).generate();
	}

}
