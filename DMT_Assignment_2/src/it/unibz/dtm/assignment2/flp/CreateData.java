package it.unibz.dtm.assignment2.flp;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Random;

import org.joda.time.DateTime;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

public class CreateData {

	public static void main(String args[]) {

		String url = "jdbc:postgresql://localhost:5432/postgres";
		String user = "postgres";
		String password = "test";
		Connection c = null;
		StringReader[] readers = { createTechdepts(), createEmployees(),
				createStudents() };

		DateTime startTime = DateTime.now();
		DateTime endTime;
		try {
			c = DriverManager.getConnection(url, user, password);
			CopyManager copyManager = new CopyManager((BaseConnection) c);
			String[] dataColumns = { "techdept", "employee", "student" };
			for (int i = 0; i < dataColumns.length; i++) {
				long result = copyManager.copyIn("COPY " + dataColumns[i]
						+ " FROM STDIN WITH DELIMITER '\t'", readers[i]);
				System.out.println(result);
			}
		} catch (SQLException | IOException ex) {
			System.out.println(ex.getMessage());
		} finally {
			endTime = DateTime.now();
			try {
				if (c != null) {
					c.close();
				}
				for (int i = 0; i < readers.length; i++) {
					if (readers[i] != null) {
						readers[i].close();
					}
				}
			} catch (SQLException ex) {
				System.out.println(ex.getMessage());
			}
		}
		System.out.println("seconds duration "
				+ (endTime.getMillis() - startTime.getMillis()) / 1000);

	}

	public static StringReader createTechdepts() {
		String techdepts = "IT\tIT-Manager\tKeller\n";
		for (int i = 0; i < 9; i++) {
			techdepts += "BWL" + i + "\tBWL-Manager" + i + "\tStrand\n";
		}
		return new StringReader(techdepts);
	}

	public static StringReader createEmployees() {
		String employees = "";
		for (int i = 0; i < 10000; i++) {
			System.out.println(i);
			Random r = new Random();
			employees += i + 1 + "\t" + generateRandomString(r) + i
					+ "\tIT-Manager\t" + "IT\t" + r.nextInt(100001) + "\t"
					+ r.nextInt(31) + "\n";
		}

		for (int i = 10000; i < 100000; i++) {
			System.out.println(i);
			Random r = new Random();
			employees += i + 1 + "\t" + generateRandomString(r) + i
					+ "\tBWL-Manager\t" + "BWL0\t" + r.nextInt(40001) + "\t"
					+ r.nextInt(101) + "\n";
		}
		return new StringReader(employees);
	}

	public static StringReader createStudents() {
		String students = "";
		for (int i = 0; i < 100000; i++) {
			System.out.println(i);
			Random r = new Random();
			students += i + 1 + "\t" + generateRandomString(r) + i + "\tDMT\t"
					+ r.nextInt(31) + "\n";
		}
		return new StringReader(students);
	}

	private static String generateRandomString(Random random) {
		String allowedChars = "abcdefghijklmnopqrstuvwxyz ";
		int max = allowedChars.length();
		StringBuffer buffer = new StringBuffer();
		for (int i = 0; i < 20; i++) {
			int value = random.nextInt(max);
			buffer.append(allowedChars.charAt(value));
		}
		return buffer.toString();
	}

}
