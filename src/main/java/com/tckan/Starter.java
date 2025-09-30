package main.java.com.tckan;
import javax.swing.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import org.w3c.dom.*;

public class Starter {
    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            try {
                // Directory selection dialog box
                JOptionPane.showMessageDialog(null, "Please select the target directory.");

                JFileChooser chooser = new JFileChooser();
                chooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
                int result = chooser.showOpenDialog(null);

                if (result != JFileChooser.APPROVE_OPTION) {
                    JOptionPane.showMessageDialog(null, "No directory selected.");
                    return;
                }

                File selectedDir = chooser.getSelectedFile();
                File resourcesXml = new File("src/main/resources/resources.xml");
                Path basePath = Paths.get(System.getProperty("user.dir"));
                Path relPath = basePath.relativize(selectedDir.toPath());
                updateConfigs(resourcesXml, "targetDirectory", relPath.toString());

                // List databases in the connection
                String[] databases = getDatabaseList();

                if (databases == null || databases.length == 0) {
                    JOptionPane.showMessageDialog(null, "No databases found.");
                    return;
                }
                
                String db = (String) JOptionPane.showInputDialog(
                        null,
                        "Please select a database:",
                        "Database Selection",
                        JOptionPane.PLAIN_MESSAGE,
                        null,
                        databases,
                        databases[0]
                );

                if (db != null) {
                    JOptionPane.showMessageDialog(null, "The selected database is: " + db);
                }

                updateConfigs(resourcesXml, "targetDatabase", db.toString());

                // Now that the configurations are set, proceed with initialising the database
                initDB();
            } catch (Exception e) {
                e.printStackTrace();
                JOptionPane.showMessageDialog(null, "Error: " + e.getMessage());
            }
        });
    }

    // Update the XML configuration file with the new path
    private static void updateConfigs(File xmlFile, String tagName, String value) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc = builder.parse(xmlFile);

        NodeList nodes = doc.getDocumentElement().getElementsByTagName(tagName);
        Element elem;

        if (nodes.getLength() > 0) {
            elem = (Element) nodes.item(0);
            elem.setTextContent(value);
        } else {
            elem = doc.createElement("config");
            elem.setAttribute("name", tagName);
            elem.setTextContent(value);
            doc.getDocumentElement().appendChild(elem);
        }

        Transformer tf = TransformerFactory.newInstance().newTransformer();
        tf.setOutputProperty(OutputKeys.INDENT, "no");
        tf.setOutputProperty(OutputKeys.METHOD, "xml");
        tf.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
        tf.transform(new DOMSource(doc), new StreamResult(xmlFile));
    }

    private static String[] getDatabaseList() {
        String tempUrl = "";
        String tempUser = "";
        String tempPassword = "";

        Path xmlPath = Paths.get("src/main/resources/resources.xml");

        try {
            String xmlContent = java.nio.file.Files.readString(xmlPath);
            try {
                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                DocumentBuilder builder = factory.newDocumentBuilder();
                ByteArrayInputStream input = new ByteArrayInputStream(xmlContent.getBytes("UTF-8"));
                Document doc = builder.parse(input);
                
                tempUrl = doc.getElementsByTagName("url").item(0).getTextContent();
                tempUser = doc.getElementsByTagName("user").item(0).getTextContent();
                tempPassword = doc.getElementsByTagName("password").item(0).getTextContent();
            } catch (Exception e) {
                System.err.println("Failed to parse XML in resources.xml: " + e.getMessage());
            }
        } catch (IOException e) {
            System.err.println("Failed to read resources.xml: " + e.getMessage());
        }
        
        final String url = tempUrl;
        final String user = tempUser;
        final String password = tempPassword;
        
        try {
            Connection conn = DriverManager.getConnection(url, user, password);
            DatabaseMetaData meta = conn.getMetaData();
            ResultSet rs = meta.getCatalogs();
            java.util.List<String> dbs = new java.util.ArrayList<>();
            
            while (rs.next()) {
                dbs.add(rs.getString(1));
            }
            
            rs.close();

            return dbs.toArray(new String[0]);
        } catch (Exception e) {
            e.printStackTrace();
            return new String[0];
        }
    }

    public static void initDB() {
        // Get the stored values of the target directory and database
        String tempTargetDirectory = "";
        String tempTargetDatabase = "";
        String tempUrl = "";
        String tempUser = "";
        String tempPassword = "";

        try {
            // Read the resources.xml file
            Path xmlPath = Paths.get("src/main/resources/resources.xml");

            try {
                String xmlContent = java.nio.file.Files.readString(xmlPath);

                try {
                    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                    DocumentBuilder builder = factory.newDocumentBuilder();
                    ByteArrayInputStream input = new ByteArrayInputStream(xmlContent.getBytes("UTF-8"));
                    Document doc = builder.parse(input);
                    
                    tempTargetDirectory = doc.getElementsByTagName("targetDirectory").item(0).getTextContent();
                    tempTargetDatabase = doc.getElementsByTagName("targetDatabase").item(0).getTextContent();
                    tempUrl = doc.getElementsByTagName("url").item(0).getTextContent();
                    tempUser = doc.getElementsByTagName("user").item(0).getTextContent();
                    tempPassword = doc.getElementsByTagName("password").item(0).getTextContent();
                } catch (Exception e) {
                    System.err.println("Failed to parse XML in resources.xml: " + e.getMessage());
                }
            } catch (IOException e) {
                System.err.println("Failed to read resources.xml: " + e.getMessage());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        final String targetDirectory = tempTargetDirectory;
        final String targetDatabase = tempTargetDatabase;
        final String url = tempUrl;
        final String user = tempUser;
        final String password = tempPassword;

        // SQL to create the 'records' table if it doesn't exist
        // Allow the value to be NULL if it cannot be parsed into a numerical value
        String createRecordsTableSql = 
                "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='records' AND xtype='U') " +
                "CREATE TABLE records (" +
                    "id INT PRIMARY KEY IDENTITY(1, 1), " +
                    "recordTime datetime NOT NULL, " +
                    "sensorName NVARCHAR(255) NOT NULL, " +
                    "value FLOAT(53), " +
                    "origDirName NVARCHAR(255) NOT NULL, " +
                    "origFileName NVARCHAR(255) NOT NULL, " +
                ")";

        try {
            // Make a connection to the database
            Connection conn = DriverManager.getConnection(url, user, password);
            Statement stmt = conn.createStatement();

            // Execute the SQL statement to create the 'records' table if it doesn't exist
            stmt.execute(createRecordsTableSql);
            System.out.println("Table checked/created successfully.");   

            processCSVFiles(targetDirectory, targetDatabase, url, user, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void processCSVFiles(String targetDirectory, String targetDatabase, String url, String user, String password) {
        CsvFileHandler csvFileHandler = new CsvFileHandler();

        try {
            // Iterate through files in the target directory
            File dir = new File(targetDirectory);
            File[] filesInDir = dir.listFiles();

            try {
                // Parallelize file processing using parallel streams
                java.util.Arrays.stream(filesInDir)
                    .parallel()
                    .filter(file -> file.isFile() && file.getName().endsWith(".csv"))
                    .forEach(file -> {
                        System.out.println("Found CSV file: " + file.getName());
                        
                        // Each thread should have its own DB connection to avoid concurrency issues
                        try (Connection threadConn = DriverManager.getConnection(url, user, password)) {
                            csvFileHandler.readCSVAndInsertToDB(targetDirectory, file, targetDatabase, threadConn);
                        } catch (SQLException e) {
                            System.err.println("Database error for file " + file.getName() + ": " + e.getMessage());
                        }
                    });
            } catch (Exception e1) {
                System.err.println("Error processing files in directory: " + e1.getMessage());
            }
        } catch (Exception e) {
            System.err.println("Error reading target directory: " + e.getMessage());
        }
    }
}