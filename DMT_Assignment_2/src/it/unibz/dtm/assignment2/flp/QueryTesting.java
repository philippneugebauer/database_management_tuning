package it.unibz.dtm.assignment2.flp;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.joda.time.DateTime;

public class QueryTesting {

	public static void main(String args[]) {

		String url = "jdbc:postgresql://localhost:5432/postgres";
		String user = "postgres";
		String password = "test";
		Connection c = null;

		DateTime startTime;
		DateTime endTime;
		try {
			c = DriverManager.getConnection(url, user, password);
			Statement s = c.createStatement();
			startTime = DateTime.now();
			s.executeQuery("SELECT AVG(salary) FROM employee WHERE dept IN (SELECT dept FROM techdept)");
			endTime = DateTime.now();
			System.out.println("seconds duration "
					+ (endTime.getMillis() - startTime.getMillis()) / 1000);

			// TODO: check, ob wirklich statement tuning
			startTime = DateTime.now();
			s.executeQuery("SELECT AVG(salary) FROM Employee, Techdept WHERE Employee.dept = Techdept.dept");
			endTime = DateTime.now();
			System.out.println("seconds duration "
					+ (endTime.getMillis() - startTime.getMillis()) / 1000);

			// FIXME: dauert viel zu lang oO
			// startTime = DateTime.now();
			// results = s
			// .executeQuery("SELECT ssnum FROM Employee e1, Techdept WHERE salary = (SELECT AVG(e2.salary) FROM Employee e2, Techdept WHERE e2.dept = e1.dept AND e2.dept = Techdept.dept)");
			// endTime = DateTime.now();
			// System.out.println("seconds duration "
			// + (endTime.getMillis() - startTime.getMillis()) / 1000);

			startTime = DateTime.now();
			s.executeUpdate("SELECT AVG(salary) as avsalary, Employee.dept INTO Temp FROM Employee, Techdept WHERE Employee.dept = Techdept.dept GROUP BY Employee.dept");
			s.executeQuery("SELECT ssnum FROM Employee, Temp WHERE salary = avsalary AND Employee.dept = Temp.dept");
			endTime = DateTime.now();
			System.out.println("seconds duration "
					+ (endTime.getMillis() - startTime.getMillis()) / 1000);
			s.executeUpdate("DROP Table temp");

		} catch (SQLException ex) {
			System.out.println(ex.getMessage());
		} finally {
			try {
				if (c != null) {
					c.close();
				}
			} catch (SQLException ex) {
				System.out.println(ex.getMessage());
			}
		}
	}
}
