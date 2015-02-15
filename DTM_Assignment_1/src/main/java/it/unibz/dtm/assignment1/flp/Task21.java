package it.unibz.dtm.assignment1.flp;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.joda.time.DateTime;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

public class Task21 {

    public static void main(String[] args) throws URISyntaxException {

        String url = "jdbc:postgresql://localhost:5432/dmt";
        String user = "postgres";
        String password = "test";
        Connection c = null;
        FileReader reader = null;

        File authFile = new File(
                "src/main/java/it/unibz/dtm/assignment1/flp/data/auth.tsv");

        DateTime startTime = DateTime.now();
        DateTime endTime;
        try {
            reader = new FileReader(authFile);
            c = DriverManager.getConnection(url, user, password);
            CopyManager copyManager = new CopyManager((BaseConnection) c);
            long result = copyManager.copyIn(
                    "COPY auth FROM STDIN WITH DELIMITER '\t'", reader);
            System.out.println(result);
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
        System.out.println("seconds duration "
                + (endTime.getMillis() - startTime.getMillis()) / 1000);
    }
}
