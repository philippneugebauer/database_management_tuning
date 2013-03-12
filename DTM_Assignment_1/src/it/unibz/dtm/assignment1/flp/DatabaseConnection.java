package it.unibz.dtm.assignment1.flp;

import it.unibz.dtm.assignment1.flp.csv.CSVReader;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DatabaseConnection {

	public static void main(String[] args) throws URISyntaxException {

		String url = "jdbc:postgresql://alcor.inf.unibz.it:5433/pneugebauer";
		String user = "pneugebauer";
		String password = "-0ppc0-<";
		Connection c = null;
		CSVReader reader = null;

		File authFile = new File(Thread.currentThread().getContextClassLoader()
				.getResource("data\\auth.tsv").toURI());
		File publFile = new File(Thread.currentThread().getContextClassLoader()
				.getResource("data\\publ.tsv").toURI());
		File[] fileArray = { authFile, publFile };
		String[] queryArray = { "Auth(name, pubID) VALUES(?, ?)",
				"Publ(pubID, type, title, booktitle, year, publisher) VALUES(?, ?, ?, ?, ?, ?)" };
		int[] attributeNumber = { 2, 6 };

		try {
			for (int i = 0; i < fileArray.length; i++) {
				reader = new CSVReader(new FileReader(fileArray[i]), '\t');
				c = DriverManager.getConnection(url, user, password);
				String[] line;
				while ((line = reader.readNext()) != null) {
					String insertQuery = "INSERT INTO " + queryArray[i];
					PreparedStatement statement = c
							.prepareStatement(insertQuery);
					for (int j = 0; j < attributeNumber[i]; j++) {
						statement.setString(j + 1, line[j]);
					}
					statement.executeUpdate();
				}
			}
		} catch (SQLException | IOException ex) {
			System.out.println(ex.getMessage());
		} finally {
			try {
				if (c != null) {
					c.close();
				}
				if (reader != null) {
					reader.close();
				}
			} catch (SQLException | IOException ex) {
				System.out.println(ex.getMessage());
			}
		}
	}
}
