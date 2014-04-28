package com.jinnova.smartpad.recom;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;

public class CatitemClusterGenerator {
	
	@SuppressWarnings("unused")
	private ClientSupport cs;
	
	public static void main(String[] args) throws SQLException, FileNotFoundException, IOException {
		
		ClientSupport cs = new ClientSupport("localhost", null, "smartpad_drill", "root", "");
		new CatitemClusterGenerator(cs).generate();
	}
	
	CatitemClusterGenerator(ClientSupport cs) {
		this.cs = cs;
	}

	void generate() {
		
	}
}
