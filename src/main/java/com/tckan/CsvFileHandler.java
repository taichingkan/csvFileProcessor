package main.java.com.tckan;
import java.util.List;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;

class CsvFileHandler {
    public void readCSVAndInsertToDB(String targetDir, File file, String targetDatabase, Connection conn) {        
        final String COMMA_DELIMITER = ",";
        List<List<String>> records = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;

            // Ensure that header line is correctly formatted
            line = br.readLine();

            if (line == null || !line.equals("timestamp,sensorName,value")) {
                System.err.println("Invalid CSV header in file: " + file.getName());
                return;
            }

            // Now, read each line and add to the List of records
            while ((line = br.readLine()) != null) {
                String[] values = line.split(COMMA_DELIMITER);
                records.add(Arrays.asList(values));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Read " + records.size() + " records from " + file.getName());

        records.forEach(record -> {
            String insertSql = "INSERT INTO records (recordTime, sensorName, value, origDirName, origFileName) VALUES (?, ?, ?, ?, ?)";

            try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                // Try to parse the timestamp given
                try {
                    pstmt.setString(1, record.get(0));
                } catch (Exception e) {
                    System.err.println("Failed to parse timestamp: " + record.get(0) + " in file: " + file.getName() + ": " + e.getMessage());

                    pstmt.setNull(1, java.sql.Types.TIMESTAMP);

                    return;
                }
                
                pstmt.setString(2, record.get(1));
                
                try {
                    pstmt.setString(3, record.get(2));
                } catch (Exception e) {
                    System.err.println("Failed to parse value: " + record.get(2) + " in file: " + file.getName() + ": " + e.getMessage());

                    pstmt.setNull(3, java.sql.Types.FLOAT);

                    return;
                }

                pstmt.setString(4, targetDir);
                pstmt.setString(5, file.getName());

                pstmt.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });

        System.out.println("Inserted data from " + file.getName() + " into database " + targetDatabase + " successfully.");

        return;
    }
}