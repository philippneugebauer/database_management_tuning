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

			startTime = DateTime.now();
			s.executeQuery("SELECT AVG(salary) FROM Employee, Techdept WHERE Employee.dept = Techdept.dept");
			endTime = DateTime.now();
			System.out.println("seconds duration "
					+ (endTime.getMillis() - startTime.getMillis()) / 1000);

			startTime = DateTime.now();
			s.executeQuery("SELECT ssnum FROM employee e1 WHERE salary = (SELECT AVG(e2.salary) FROM employee e2, techdept WHERE e2.dept = e1.dept AND e2.dept = techdept.dept)");
			endTime = DateTime.now();
			System.out.println("seconds duration "
					+ (endTime.getMillis() - startTime.getMillis()) / 1000);

			startTime = DateTime.now();
			s.executeUpdate("SELECT AVG(salary) as avsalary, employee.dept INTO TEMPORARY TABLE temp FROM employee, techdept WHERE employee.dept = techdept.dept GROUP BY employee.dept");
			s.executeQuery("SELECT ssnum FROM employee, temp WHERE salary = avsalary AND employee.dept = temp.dept");
			endTime = DateTime.now();
			System.out.println("seconds duration "
					+ (endTime.getMillis() - startTime.getMillis()) / 1000);

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
