import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DaliyData {

    private static final String DB_URL = "jdbc:mysql://localhost:3306/nepse_demodata";
    private static final String DB_USER = "root";
    private static final String DB_PASS = "";
    private static final long INTERVAL = 60000;
    private static final LocalTime START_OF_DAY = LocalTime.of(7, 45);
    private static final LocalTime END_OF_DAY = LocalTime.of(15, 15);

    private static final Logger LOGGER = Logger.getLogger(DaliyData.class.getName());

    private static String lastHash = "";

    public static void main(String[] args) {
        try {
            createTableIfNotExists();
            LOGGER.info("Table checked/created successfully."); // Add logging
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Error creating table: ", e);
            return;
        }

        while (true) {
            try {
                LocalTime now = LocalTime.now();
                if (now.isBefore(START_OF_DAY) || now.isAfter(END_OF_DAY)) {
                    if (now.isAfter(END_OF_DAY)) {
                        storeLastUpdateOfTheDay();
                        LOGGER.info("Last update of the day recorded at " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))); // Add logging
                        lastHash = "";
                    }
                    LOGGER.info("Market is closed. Sleeping until next check."); // Add logging
                    Thread.sleep(getSleepDuration());
                    continue;
                }

                String currentDataURL = "https://nepsealpha.com/live-market";
                String content = fetchData(currentDataURL);
                String currentHash = generateHash(content);

                if (!currentHash.equals(lastHash)) {
                    scrapeAndStoreData(content);
                    lastHash = currentHash;
                    LocalDateTime nowDateTime = LocalDateTime.now();
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    String formattedNow = nowDateTime.format(formatter);
                    LOGGER.info("Data updated successfully at " + formattedNow); // Add logging
                } else {
                    LOGGER.info("Data remains the same. Skipping database update."); // Add logging
                }
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error fetching or storing data: ", e);
            }

            try {
                Thread.sleep(INTERVAL);
            } catch (InterruptedException e) {
                LOGGER.log(Level.SEVERE, "Thread interrupted: ", e); // Add logging
                Thread.currentThread().interrupt();
            }
        }
    }

    private static long getSleepDuration() {
        LocalTime now = LocalTime.now();
        LocalTime nextStart = now.isBefore(START_OF_DAY) ? START_OF_DAY : START_OF_DAY.plusHours(24);
        return java.time.Duration.between(now, nextStart).toMillis();
    }

    private static String fetchData(String urlStr) throws IOException {
        StringBuilder result = new StringBuilder();
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("User-Agent", "Mozilla/5.0");

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                result.append(line);
            }
        }
        return result.toString();
    }

    private static String generateHash(String content) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(content.getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(hash);
    }

    private static void createTableIfNotExists() throws SQLException {
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
             Statement stmt = conn.createStatement()) {

            String liveTableSQL = "CREATE TABLE IF NOT EXISTS live_data (" +
                    "id INT AUTO_INCREMENT PRIMARY KEY," +
                    "date DATE," +
                    "symbol VARCHAR(255)," +
                    "LTP DECIMAL(10,2)," +
                    "open DECIMAL(10,2)," +
                    "high DECIMAL(10,2)," +
                    "low DECIMAL(10,2)," +
                    "close DECIMAL(10,2)," +
                    "perChange DECIMAL(10,2)," +
                    "vol DECIMAL(20,2)," +
                    "Ly DECIMAL(10,2)," +
                    "turnover DECIMAL(20,2)," +
                    "UNIQUE KEY unique_date_symbol (date, symbol)" +
                    ")";
            stmt.executeUpdate(liveTableSQL);
        }
    }

    private static void scrapeAndStoreData(String content) throws SQLException {
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
            Document doc = Jsoup.parse(content);
            LOGGER.info("Parsed HTML content successfully.");
            for (Element row : doc.select("table.table.table-hover.table-striped.mb-0.table-bordered.no-footer.dataTable tr")) {
                LOGGER.info("Processing row: " + row.text());
                Elements cells = row.select("td");

                if (cells.size() < 8) {
                    System.out.println("Skipping row with insufficient columns: " + row.text());
                    continue;
                }

                String symbol = cells.get(0).text().trim();
                if (symbol.isEmpty()) {
                    System.out.println("Skipping row with empty symbol: " + row.text());

                    continue;
                }

                try {
                    LocalDate date = LocalDate.now();
                    double perChange = parseDouble(cells.get(1).text());
                    double open = parseDouble(cells.get(2).text());
                    double high = parseDouble(cells.get(3).text());
                    double low = parseDouble(cells.get(4).text());
                    double close = parseDouble(cells.get(5).text());
                    double turnover = parseDouble(cells.get(6).text());
                    double vol = parseDouble(cells.get(7).text());

                    System.out.println("datas : "+open+high+symbol);


                    String upsertSql = "INSERT INTO live_data (date, symbol, LTP, open, high, low, close, perChange, vol, Ly, turnover) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                            "ON DUPLICATE KEY UPDATE LTP = VALUES(LTP), open = VALUES(open), high = VALUES(high), low = VALUES(low), close = VALUES(close), perChange = VALUES(perChange), vol = VALUES(vol), Ly = VALUES(Ly), turnover = VALUES(turnover)";

                    try (PreparedStatement pstmt = conn.prepareStatement(upsertSql)) {
                        pstmt.setObject(1, date);
                        pstmt.setString(2, symbol);
                        pstmt.setDouble(3, close);
                        pstmt.setDouble(4, open);
                        pstmt.setDouble(5, high);
                        pstmt.setDouble(6, low);
                        pstmt.setDouble(7, close);
                        pstmt.setDouble(8, perChange);
                        pstmt.setDouble(9, vol);
                        pstmt.setDouble(10, 0);
                        pstmt.setDouble(11, turnover);
                        int affectedRows = pstmt.executeUpdate();
                        if (affectedRows > 0) {
                            System.out.println("Data inserted/updated for symbol: " + symbol);
                        } else {
                            System.out.println("No data inserted/updated for symbol: " + symbol);
                        }
                    }
                } catch (NumberFormatException e) {
                    System.err.println("Skipping row due to number format error: " + e.getMessage() + " - Row data: " + row.text());
                } catch (SQLException e) {
                    System.err.println("SQL error while inserting row: " + e.getMessage() + " - Row data: " + row.text());
                }
            }
        }
    }

    private static double parseDouble(String text) {
        try {
            return Double.parseDouble(text.replace(",", "").replace("-", "0"));
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    private static void storeLastUpdateOfTheDay() throws SQLException {
        LocalDate date = LocalDate.now();
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
            String selectSql = "SELECT DISTINCT symbol FROM live_data";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(selectSql)) {
                while (rs.next()) {
                    String symbol = rs.getString("symbol");
                    String tableName = "daily_data_" + symbol.replaceAll("\\W", "_");

                    if (!tableExists(conn, tableName)) {
                        String createTableSql = "CREATE TABLE " + tableName + " (" +
                                "date DATE," +
                                "open DOUBLE," +
                                "high DOUBLE," +
                                "low DOUBLE," +
                                "close DOUBLE," +
                                "volume DOUBLE," +
                                "turnover DOUBLE," +
                                "PRIMARY KEY (date)" +
                                ")";
                        try (Statement createStmt = conn.createStatement()) {
                            createStmt.executeUpdate(createTableSql);
                        }
                    }

                    String insertSql = "INSERT INTO " + tableName + " (date, open, high, low, close, volume, turnover) " +
                            "SELECT date, open, high, low, close, vol, turnover FROM live_data WHERE symbol = ? AND date = ?";
                    try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                        pstmt.setString(1, symbol);
                        pstmt.setObject(2, date);
                        pstmt.executeUpdate();
                    }
                }
            }
        }
    }

    private static boolean tableExists(Connection conn, String tableName) throws SQLException {
        try (ResultSet rs = conn.getMetaData().getTables(null, null, tableName, null)) {
            return rs.next();
        }
    }
}

