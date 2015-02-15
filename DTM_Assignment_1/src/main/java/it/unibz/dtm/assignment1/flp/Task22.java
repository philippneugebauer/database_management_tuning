package it.unibz.dtm.assignment1.flp;

import it.unibz.dtm.assignment1.flp.csv.CSVReader;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.joda.time.DateTime;

public class Task22 {

    public static void main(String[] args) {
        String url = "jdbc:postgresql://localhost:5432/dmt";
        String user = "postgres";
        String password = "test";
        Connection c = null;
        CSVReader reader = null;

        File authFile = new File(
                "src/main/java/it/unibz/dtm/assignment1/flp/data/auth.tsv");
        String query = "INSERT INTO auth(name, pubid) VALUES(?,?)";

        DateTime startTime = DateTime.now();
        DateTime endTime;
        try {
            c = DriverManager.getConnection(url, user, password);
            c.setAutoCommit(false);
            PreparedStatement st = c.prepareStatement(query);

            reader = new CSVReader(new FileReader(authFile), '\t');
            String[] line;
            int counter = 0;
            while ((line = reader.readNext()) != null) {
                counter++;
                for (int j = 0; j < 2; j++) {
                    st.setString(j + 1, line[j]);
                }
                st.addBatch();
                if ((counter % 80) == 0) {
                    st.executeBatch();
                }
            }
            st.executeBatch();
            c.commit();

        } catch (SQLException e) {
            System.out.println(e.getMessage());
            if (e.getNextException() != null) {
                System.out.println(e.getNextException().getMessage());
            }
        } catch (IOException ex) {
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
        System.out.println("seconds duration "
                + (endTime.getMillis() - startTime.getMillis()) / 1000);
    }
}
