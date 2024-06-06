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
import java.util.HashMap;
import java.util.Map;

public class dailyData {

    private static final String DB_URL = "jdbc:mysql://localhost:3306/nepse_livedata";
    private static final String DB_USER = "root";
    private static final String DB_PASS = "";
    private static final long INTERVAL = 60000;
    private static final LocalTime START_OF_DAY = LocalTime.of(11, 00);
    private static final LocalTime END_OF_DAY = LocalTime.of(15, 01);

    private static String lastHash = "";

    public static void main(String[] args) {
        try {
            createTableIfNotExists();
            System.out.println("Table checked/created successfully.");
        } catch (SQLException e) {
            System.err.println("Error creating table: " + e.getMessage());
            return;
        }

        while (true) {
            try {
                LocalTime now = LocalTime.now();
                if (now.isBefore(START_OF_DAY) || now.isAfter(END_OF_DAY)) {
                    if (now.isAfter(END_OF_DAY)) {
                        storeLastUpdateOfTheDay();
                        System.out.println("Last update of the day recorded at " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                        lastHash = "";
                    }
                    System.out.println("Market is closed. Sleeping until next check.");
                    Thread.sleep(getSleepDuration());
                    continue;
                }

                String currentDataURL = "https://www.sharesansar.com/today-share-price";
                String content = fetchData(currentDataURL);
                String currentHash = generateHash(content);

                if (!currentHash.equals(lastHash)) {
                    scrapeAndStoreData(content);
                    lastHash = currentHash;
                    LocalDateTime nowDateTime = LocalDateTime.now();
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    String formattedNow = nowDateTime.format(formatter);
                    System.out.println("Data updated successfully at " + formattedNow);
                } else {
                    System.out.println("Data remains the same. Skipping database update.");
                }
            } catch (Exception e) {
                System.err.println("Error fetching or storing data: " + e.getMessage());
            }

            try {
                Thread.sleep(INTERVAL);
            } catch (InterruptedException e) {
                System.err.println("Thread interrupted: " + e.getMessage());
                Thread.currentThread().interrupt();
            }
        }
    }

    private static long getSleepDuration() {
        LocalTime now = LocalTime.now();
        LocalTime nextStart = now.isBefore(START_OF_DAY) ? START_OF_DAY : START_OF_DAY.plusHours(24);
        return now.isAfter(nextStart) ? java.time.Duration.between(nextStart, now).toMillis() : java.time.Duration.between(now, nextStart).toMillis();
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

            String dailyTableSQL = "CREATE TABLE IF NOT EXISTS daily_data (" +
                    "id INT AUTO_INCREMENT PRIMARY KEY," +
                    "date DATE," +
                    "symbol VARCHAR(255)," +
                    "conf DECIMAL(10,2)," +
                    "open DECIMAL(10,2)," +
                    "high DECIMAL(10,2)," +
                    "low DECIMAL(10,2)," +
                    "close DECIMAL(10,2)," +
                    "vwap VARCHAR(20)," +
                    "vol DECIMAL(20,2)," +
                    "prev_close DECIMAL(10,2)," +
                    "turnover DECIMAL(20,2)," +
                    "trans INT," +
                    "diff VARCHAR(20)," +
                    "`range` DECIMAL(10,2)," +
                    "diff_perc VARCHAR(20)," +
                    "range_perc DECIMAL(10,2)," +
                    "vwap_perc VARCHAR(20)," +
                    "days_120 DECIMAL(10,2)," +
                    "days_180 DECIMAL(10,2)," +
                    "weeks_52_high DECIMAL(10,2)," +
                    "weeks_52_low DECIMAL(10,2)," +
                    "UNIQUE KEY unique_date_symbol (date, symbol)" +
                    ")";
            stmt.executeUpdate(dailyTableSQL);
        }
    }

    private static void scrapeAndStoreData(String content) throws SQLException {
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
            Document doc = Jsoup.parse(content);
            for (Element row : doc.select("table.table tr")) {
                Elements cells = row.select("td");

                if (cells.size() < 2) {
                    System.out.println("Skipping row with insufficient columns: " + row.text());
                    continue;
                }

                String symbol = cells.get(1).text().trim();
                if (symbol.isEmpty()) {
                    System.out.println("Skipping row with empty symbol: " + row.text());
                    continue;
                }

                while (cells.size() < 21) {
                    cells.add(new Element("td").text("0"));
                }

                try {
                    LocalDate date = LocalDate.now();
                    double conf = parseDouble(cells.get(2).text());
                    double open = parseDouble(cells.get(3).text());
                    double high = parseDouble(cells.get(4).text());
                    double low = parseDouble(cells.get(5).text());
                    double close = parseDouble(cells.get(6).text());
                    String vwap = cells.get(7).text();
                    double vol = parseDouble(cells.get(8).text());
                    double prevClose = parseDouble(cells.get(9).text());
                    double turnover = parseDouble(cells.get(10).text());
                    int trans = parseInt(cells.get(11).text());
                    String diff = cells.get(12).text();
                    double range = parseDouble(cells.get(13).text());
                    String diffPerc = cells.get(14).text();
                    double rangePerc = parseDouble(cells.get(15).text());
                    String vwapPerc = cells.get(16).text();
                    double days120 = parseDouble(cells.get(17).text());
                    double days180 = parseDouble(cells.get(18).text());
                    double weeks52High = parseDouble(cells.get(19).text());
                    double weeks52Low = parseDouble(cells.get(20).text());

                    String checkSql = "SELECT COUNT(*) FROM daily_data WHERE symbol = ?";
                    try (PreparedStatement checkStmt = conn.prepareStatement(checkSql)) {
                        checkStmt.setString(1, symbol);
                        try (ResultSet rs = checkStmt.executeQuery()) {
                            if (rs.next() && rs.getInt(1) > 0) {
                                String updateSql = "UPDATE daily_data SET date = ?, conf = ?, open = ?, high = ?, low = ?, close = ?, vwap = ?, vol = ?, prev_close = ?, turnover = ?, trans = ?, diff = ?, `range` = ?, diff_perc = ?, range_perc = ?, vwap_perc = ?, days_120 = ?, days_180 = ?, weeks_52_high = ?, weeks_52_low = ? WHERE symbol = ?";
                                try (PreparedStatement pstmt = conn.prepareStatement(updateSql)) {
                                    pstmt.setObject(1, date);
                                    pstmt.setDouble(2, conf);
                                    pstmt.setDouble(3, open);
                                    pstmt.setDouble(4, high);
                                    pstmt.setDouble(5, low);
                                    pstmt.setDouble(6, close);
                                    pstmt.setString(7, vwap);
                                    pstmt.setDouble(8, vol);
                                    pstmt.setDouble(9, prevClose);
                                    pstmt.setDouble(10, turnover);
                                    pstmt.setInt(11, trans);
                                    pstmt.setString(12, diff);
                                    pstmt.setDouble(13, range);
                                    pstmt.setString(14, diffPerc);
                                    pstmt.setDouble(15, rangePerc);
                                    pstmt.setString(16, vwapPerc);
                                    pstmt.setDouble(17, days120);
                                    pstmt.setDouble(18, days180);
                                    pstmt.setDouble(19, weeks52High);
                                    pstmt.setDouble(20, weeks52Low);
                                    pstmt.setString(21, symbol);
                                    pstmt.executeUpdate();
                                }
                            } else {
                                String insertSql = "INSERT INTO daily_data (date, symbol, conf, open, high, low, close, vwap, vol, prev_close, turnover, trans, diff, `range`, diff_perc, range_perc, vwap_perc, days_120, days_180, weeks_52_high, weeks_52_low) " +
                                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                                try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                                    pstmt.setObject(1, date);
                                    pstmt.setString(2, symbol);
                                    pstmt.setDouble(3, conf);
                                    pstmt.setDouble(4, open);
                                    pstmt.setDouble(5, high);
                                    pstmt.setDouble(6, low);
                                    pstmt.setDouble(7, close);
                                    pstmt.setString(8, vwap);
                                    pstmt.setDouble(9, vol);
                                    pstmt.setDouble(10, prevClose);
                                    pstmt.setDouble(11, turnover);
                                    pstmt.setInt(12, trans);
                                    pstmt.setString(13, diff);
                                    pstmt.setDouble(14, range);
                                    pstmt.setString(15, diffPerc);
                                    pstmt.setDouble(16, rangePerc);
                                    pstmt.setString(17, vwapPerc);
                                    pstmt.setDouble(18, days120);
                                    pstmt.setDouble(19, days180);
                                    pstmt.setDouble(20, weeks52High);
                                    pstmt.setDouble(21, weeks52Low);
                                    pstmt.executeUpdate();
                                }
                            }
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

    private static int parseInt(String text) {
        try {
            return Integer.parseInt(text.replace(",", "").replace("-", "0"));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static boolean tableExists(Connection conn, String tableName) {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeQuery("SELECT 1 FROM " + tableName + " LIMIT 1");
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    private static void storeLastUpdateOfTheDay() throws SQLException {
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
            Map<String, Integer> updateCounts = new HashMap<>();  // Initialize map to store update counts for each table
            String selectSql = "SELECT DISTINCT symbol, date FROM daily_data";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(selectSql)) {
                while (rs.next()) {
                    String symbol = rs.getString("symbol");
                    LocalDate date = rs.getDate("date").toLocalDate();
                    String tableName = "daily_data_" + symbol.replaceAll("\\W", "_").toLowerCase();  // Normalize to lowercase

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
                            System.out.println("\u001B[32m"+"Table created: " + tableName+"\u001B[0m");
                        }
                    }

                    String insertSql = "INSERT INTO " + tableName + " (date, open, high, low, close, volume, turnover) " +
                            "SELECT date, open, high, low, close, vol, turnover FROM daily_data WHERE symbol = ? AND date = ? " +
                            "ON DUPLICATE KEY UPDATE " +
                            "open = VALUES(open), high = VALUES(high), low = VALUES(low), close = VALUES(close), volume = VALUES(volume), turnover = VALUES(turnover)";
                    try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                        pstmt.setString(1, symbol);
                        pstmt.setObject(2, date);
                        int rowsAffected = pstmt.executeUpdate();
                        if (rowsAffected > 0) {
                            updateCounts.put(tableName, updateCounts.getOrDefault(tableName, 0) + 1);  // Increment count for tableName
                        }
                        System.out.println("Table updated: " + tableName + " Updated count: " + updateCounts.get(tableName));
                    }
                }
            }
            System.out.println("Total updates for each table:");
            for (Map.Entry<String, Integer> entry : updateCounts.entrySet()) {
                System.out.println("Table " + entry.getKey() + " updated " + entry.getValue() + " times");
            }
        }
    }



    private static Map<String, Object> getLastData(String symbol) throws SQLException {
        Map<String, Object> data = new HashMap<>();
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
            String tableName = "daily_data_" + symbol.replace("-", "_");
            String sql = "SELECT * FROM " + tableName + " WHERE date = (SELECT MAX(date) FROM " + tableName + ")";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    ResultSetMetaData rsmd = rs.getMetaData();
                    int columnCount = rsmd.getColumnCount();
                    for (int i = 1; i <= columnCount; i++) {
                        String name = rsmd.getColumnName(i);
                        Object value = rs.getObject(i);
                        data.put(name, value);
                    }
                }
            }
        }
        return data;
    }
}
