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

import org.joda.time.DateTime;

public class Task1 {

    public static void main(String[] args) throws URISyntaxException {

        String url = "jdbc:postgresql://localhost:5432/dmt";
        String user = "postgres";
        String password = "test";
        Connection c = null;
        CSVReader reader = null;

        File authFile = new File(
                "src/main/java/it/unibz/dtm/assignment1/flp/data/auth.tsv");
        String insertQuery = "INSERT INTO auth(name, pubid) VALUES(?, ?)";

        DateTime startTime = DateTime.now();
        DateTime endTime;
        try {
            reader = new CSVReader(new FileReader(authFile), '\t');
            c = DriverManager.getConnection(url, user, password);
            String[] line;
            while ((line = reader.readNext()) != null) {
                PreparedStatement statement = c.prepareStatement(insertQuery);
                for (int j = 0; j < 2; j++) {
                    statement.setString(j + 1, line[j]);
                }
                if (c.isValid(10)) {
                } else {
                    try {
                        if (c != null) {
                            c.close();
                        }
                        c = DriverManager.getConnection(url, user, password);
                    } catch (SQLException ex) {
                        System.out.println(ex.getMessage());
                    }
                }
                statement.executeUpdate();
            }
        } catch (SQLException | IOException ex) {
            System.out.println(ex.getMessage());
        } finally {
            endTime = DateTime.now();
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
        System.out.println("start_time: "
                + startTime.toString("dd.MM.yyyy HH:mm:ss"));
        System.out.println("end_time: "
                + endTime.toString("dd.MM.yyyy HH:mm:ss"));
        System.out.println("millis duration "
                + (endTime.getMillis() - startTime.getMillis()));
        System.out.println("millis duration "
                + (endTime.getMillis() - startTime.getMillis()));
        System.out.println("seconds duration "
                + (endTime.getMillis() - startTime.getMillis()) / 1000);
    }
}
