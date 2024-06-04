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

public class liveData {

    private static final String DB_URL = "jdbc:mysql://localhost:3306/nepse_data";
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

                String currentDataURL = "https://www.sharesansar.com/live-trading";
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

            String liveTableSQL = "CREATE TABLE IF NOT EXISTS live_data (" +
                    "id INT AUTO_INCREMENT PRIMARY KEY," +
                    "date DATE," +
                    "symbol VARCHAR(255)," +
                    "ltp DECIMAL(10,2)," +
                    "pointChange VARCHAR(20)," +
                    "perChange VARCHAR(20)," +
                    "open DECIMAL(10,2)," +
                    "high DECIMAL(10,2)," +
                    "low DECIMAL(10,2)," +
                    "vol DECIMAL(20,2)," +
                    "prev_close DECIMAL(10,2)," +
                    "UNIQUE KEY unique_date_symbol (date, symbol)" +
                    ")";
            stmt.executeUpdate(liveTableSQL);
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
                    double ltp = parseDouble(cells.get(2).text());
                    String pointChange = cells.get(3).text();
                    String perChange = cells.get(4).text();
                    double open = parseDouble(cells.get(5).text());
                    double high = parseDouble(cells.get(6).text());
                    double low = parseDouble(cells.get(7).text());
                    double vol = parseDouble(cells.get(8).text());
                    double prevClose = parseDouble(cells.get(9).text());

                    String checkSql = "SELECT COUNT(*) FROM live_data WHERE symbol = ?";
                    try (PreparedStatement checkStmt = conn.prepareStatement(checkSql)) {
                        checkStmt.setString(1, symbol);
                        try (ResultSet rs = checkStmt.executeQuery()) {
                            if (rs.next() && rs.getInt(1) > 0) {
                                String updateSql = "UPDATE live_data SET date = ?, ltp = ?, pointChange = ?, perChange = ?, open = ?, high = ?, low = ?, vol = ?, prev_close = ? WHERE symbol = ?";
                                try (PreparedStatement pstmt = conn.prepareStatement(updateSql)) {
                                    pstmt.setObject(1, date);
                                    pstmt.setDouble(2, ltp);
                                    pstmt.setString(3, pointChange);
                                    pstmt.setString(4, perChange);
                                    pstmt.setDouble(5, open);
                                    pstmt.setDouble(6, high);
                                    pstmt.setDouble(7, low);
                                    pstmt.setDouble(8, vol);
                                    pstmt.setDouble(9, prevClose);
                                    pstmt.setString(10, symbol);
                                    pstmt.executeUpdate();
                                }
                            } else {
                                String insertSql = "INSERT INTO live_data (date, symbol, ltp, pointChange, perChange, open, high, low, vol, prev_close) " +
                                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                                try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                                    pstmt.setObject(1, date);
                                    pstmt.setString(2, symbol);
                                    pstmt.setDouble(3, ltp);
                                    pstmt.setString(4, pointChange);
                                    pstmt.setString(5, perChange);
                                    pstmt.setDouble(6, open);
                                    pstmt.setDouble(7, high);
                                    pstmt.setDouble(8, low);
                                    pstmt.setDouble(9, vol);
                                    pstmt.setDouble(10, prevClose);
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
                                "turnover Double," +
                                "volume Double," +
                                "PRIMARY KEY (date)" +
                                ")";
                        System.out.println("Creating table with SQL: " + createTableSql); //debugging: check table is create or not
                        stmt.executeUpdate(createTableSql);
                    }

                    String insertSql = "INSERT INTO " + tableName + " (date, open, high, low, close, volume,turnover) " +
                            "SELECT ?, open, high, low, close, turnover,CASE WHEN vol >= 0 THEN vol ELSE 0 END AS volume " +
                            "FROM live_data " +
                            "WHERE symbol = ? AND date = (SELECT MAX(date) FROM live_data WHERE symbol = ?) " +
                            "ON DUPLICATE KEY UPDATE open = VALUES(open), high = VALUES(high), low = VALUES(low), close = VALUES(close), turnover = VALUES(turnover),volume = CASE WHEN VALUES(volume) >= 0 THEN VALUES(volume) ELSE 0 END";

//                    System.out.println("Inserting data with SQL: " + insertSql); //debugging: check table is update or not

                    try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                        pstmt.setObject(1, date.atTime(END_OF_DAY));
                        pstmt.setString(2, symbol);
                        pstmt.setString(3, symbol);
                        int rowsAffected = pstmt.executeUpdate();
//                        System.out.println("Rows affected: " + rowsAffected); //debugging: number of row affected by query
                    }
                }
            }
        }
    }

    private static boolean tableExists(Connection conn, String tableName) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet rs = meta.getTables(null, null, tableName, new String[]{"TABLE"})) {
            return rs.next();
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