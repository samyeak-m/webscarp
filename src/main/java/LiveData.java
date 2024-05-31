import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.util.Map;
import java.util.HashMap;
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
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.regex.Pattern;

import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;


public class LiveData {

    private static final String DB_URL = "jdbc:mysql://localhost:3306/nepse_livedata";
    private static final String DB_USER = "root";
    private static final String DB_PASS = "";
    private static final long INTERVAL = 60000;
    private static final LocalTime START_OF_DAY = LocalTime.of(10, 45);
    private static final LocalTime END_OF_DAY = LocalTime.of(15, 15);

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
                        lastHash = ""; // Reset last hash for the next day
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
                    System.out.println("No changes detected");
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
                    "symbol VARCHAR(255)," +
                    "conf DECIMAL(10,2)," +
                    "open DECIMAL(10,2)," +
                    "high DECIMAL(10,2)," +
                    "low DECIMAL(10,2)," +
                    "close DECIMAL(10,2)," +
                    "vwap DECIMAL(10,2)," +
                    "vol BIGINT," +
                    "prev_close DECIMAL(10,2)," +
                    "turnover DECIMAL(10,2)," +
                    "trans INT," +
                    "diff DECIMAL(10,2)," +
                    "`range` DECIMAL(10,2)," +
                    "diff_perc DECIMAL(5,2)," +
                    "range_vwap DECIMAL(10,2)," +
                    "days_120 DECIMAL(10,2)," +
                    "days_180 DECIMAL(10,2)," +
                    "weeks_52_high DECIMAL(10,2)," +
                    "weeks_52_low DECIMAL(10,2)" +
                    ")";
            stmt.executeUpdate(liveTableSQL);
        }
    }

    private static void scrapeAndStoreData(String content) throws SQLException {
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
            Document doc = Jsoup.parse(content);
            for (Element row : doc.select("table.table tr")) {
                Elements cells = row.select("td");
                if (cells.size() >= 20) {
                    try {
                        LocalDate date = LocalDate.now();
                        String symbol = cells.get(1).text();
                        String confText = cells.get(2).text().replace(",", "");
                        double conf = confText.equals("-") ? 0.0 : Double.parseDouble(confText);
                        double open = Double.parseDouble(cells.get(3).text().replace(",", ""));
                        double high = Double.parseDouble(cells.get(4).text().replace(",", ""));
                        double low = Double.parseDouble(cells.get(5).text().replace(",", ""));
                        double close = Double.parseDouble(cells.get(6).text().replace(",", ""));
                        double vwap = Double.parseDouble(cells.get(7).text().replace(",", ""));
                        long vol = Long.parseLong(cells.get(8).text().replace(",", ""));
                        double prevClose = Double.parseDouble(cells.get(9).text().replace(",", ""));
                        double turnover = Double.parseDouble(cells.get(10).text().replace(",", ""));
                        int trans = Integer.parseInt(cells.get(11).text().replace(",", ""));
                        double diff = Double.parseDouble(cells.get(12).text().replace(",", ""));
                        double range = Double.parseDouble(cells.get(13).text().replace(",", ""));
                        double diffPerc = Double.parseDouble(cells.get(14).text().replace(",", ""));
                        double rangeVwap = Double.parseDouble(cells.get(15).text().replace(",", ""));
                        double days120 = Double.parseDouble(cells.get(16).text().replace(",", ""));
                        double days180 = Double.parseDouble(cells.get(17).text().replace(",", ""));
                        double weeks52High = Double.parseDouble(cells.get(18).text().replace(",", ""));
                        double weeks52Low = Double.parseDouble(cells.get(19).text().replace(",", ""));

                        String sql = "INSERT INTO live_data (date, symbol, conf, open, high, low, close, vwap, vol, prev_close, turnover, trans, diff, `range`, diff_perc, range_vwap, days_120, days_180, weeks_52_high, weeks_52_low) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                                "ON DUPLICATE KEY UPDATE open = VALUES(open), high = VALUES(high), low = VALUES(low), close = VALUES(close), vol = VALUES(vol), prev_close = VALUES(prev_close), turnover = VALUES(turnover), trans = VALUES(trans), diff = VALUES(diff), `range` = VALUES(`range`), diff_perc = VALUES(diff_perc), range_vwap = VALUES(range_vwap), days_120 = VALUES(days_120), days_180 = VALUES(days_180), weeks_52_high = VALUES(weeks_52_high), weeks_52_low = VALUES(weeks_52_low)";
                        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                            pstmt.setObject(1, date);
                            pstmt.setString(2, symbol);
                            pstmt.setDouble(3, conf);
                            pstmt.setDouble(4, open);
                            pstmt.setDouble(5, high);
                            pstmt.setDouble(6, low);
                            pstmt.setDouble(7, close);
                            pstmt.setDouble(8, vwap);
                            pstmt.setLong(9, vol);
                            pstmt.setDouble(10, prevClose);
                            pstmt.setDouble(11, turnover);
                            pstmt.setInt(12, trans);
                            pstmt.setDouble(13, diff);
                            pstmt.setDouble(14, range);
                            pstmt.setDouble(15, diffPerc);
                            pstmt.setDouble(16, rangeVwap);
                            pstmt.setDouble(17, days120);
                            pstmt.setDouble(18, days180);
                            pstmt.setDouble(19, weeks52High);
                            pstmt.setDouble(20, weeks52Low);
                            pstmt.executeUpdate();
                        } catch (NumberFormatException e) {
                            System.err.println("Skipping row due to number format error: " + e.getMessage());
                        }
                    } catch (NumberFormatException e) {
                        System.err.println("Skipping row due to number format error: " + e.getMessage());
                    }
                } else {
                    System.out.println("Skipping row with insufficient columns: " + row.text());
                }
            }
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
                    String tableName = "daily_data_" + symbol.replace("-", "_"); // Create a valid table name

                    // Create table if not exists
                    String createTableSql = "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                            "date DATE," +
                            "open DOUBLE," +
                            "high DOUBLE," +
                            "low DOUBLE," +
                            "close DOUBLE," +
                            "volume BIGINT," +
                            "PRIMARY KEY (date)" +
                            ")";
                    stmt.executeUpdate(createTableSql);

                    // Insert or update data
                    String insertSql = "INSERT INTO " + tableName + " (date, open, high, low, close, volume) " +
                            "SELECT ?, open, high, low, close, volume FROM live_data " +
                            "WHERE symbol = ? AND date = (SELECT MAX(date) FROM live_data WHERE symbol = ?) " +
                            "ON DUPLICATE KEY UPDATE open = VALUES(open), high = VALUES(high), low = VALUES(low), close = VALUES(close), volume = VALUES(volume)";
                    try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                        pstmt.setObject(1, date.atTime(END_OF_DAY));
                        pstmt.setString(2, symbol);
                        pstmt.setString(3, symbol);
                        pstmt.executeUpdate();
                    }
                }
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