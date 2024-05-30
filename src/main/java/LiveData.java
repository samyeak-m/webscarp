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

public class LiveData {

    private static final String DB_URL = "jdbc:mysql://localhost:3306/nepse_livedata";
    private static final String DB_USER = "root";
    private static final String DB_PASS = "";
    private static final long INTERVAL = 60000;
    private static final LocalTime END_OF_DAY = LocalTime.of(16, 30);

    private static String lastHash = "";

    public static void main(String[] args) {
        try {
            createTableIfNotExists();
        } catch (SQLException e) {
            System.err.println("Error creating table: " + e.getMessage());
            return;
        }

        while (true) {
            try {
                String currentDataURL = "https://www.sharesansar.com/today-share-price";
                String content = fetchData(currentDataURL);
                String currentHash = generateHash(content);

                if (!currentHash.equals(lastHash)) {
                    scrapeAndStoreData(content);
                    lastHash = currentHash;
                    LocalDateTime now = LocalDateTime.now();
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    String formattedNow = now.format(formatter);
                    System.out.println("Data updated successfully at " + formattedNow);
                } else {
                    LocalDateTime now = LocalDateTime.now();
                    if (now.toLocalTime().isAfter(END_OF_DAY)) {
                        storeLastUpdateOfTheDay();
                        System.out.println("Last update of the day recorded at " + now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                        lastHash = ""; // Reset last hash for the next day
                    } else {
                        System.out.println("No changes detected");
                    }
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

            String sql = "CREATE TABLE IF NOT EXISTS nepse_data (" +
                    "id INT AUTO_INCREMENT PRIMARY KEY," +
                    "date DATE," +
                    "symbol VARCHAR(255)," +
                    "open DECIMAL(10,2)," +
                    "high DECIMAL(10,2)," +
                    "low DECIMAL(10,2)," +
                    "close DECIMAL(10,2)," +
                    "volume BIGINT" +
                    ")";
            stmt.executeUpdate(sql);
        }
    }

    private static void scrapeAndStoreData(String content) throws SQLException {
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
            Document doc = Jsoup.parse(content);
            for (Element row : doc.select("table.table tr")) {
                Elements cells = row.select("td");
                if (cells.size() >= 7) {
                    LocalDate date = LocalDate.now();
                    String symbol = cells.get(1).text();
                    double open = Double.parseDouble(cells.get(2).text().replace(",", ""));
                    double high = Double.parseDouble(cells.get(3).text().replace(",", ""));
                    double low = Double.parseDouble(cells.get(4).text().replace(",", ""));
                    double close = Double.parseDouble(cells.get(5).text().replace(",", ""));
                    double volumeDouble = Double.parseDouble(cells.get(6).text().replace(",", ""));
                    long volume = (long) volumeDouble;

                    String sql = "INSERT INTO nepse_data (date, symbol, open, high, low, close, volume) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?)";
                    try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                        pstmt.setObject(1, date);
                        pstmt.setString(2, symbol);
                        pstmt.setDouble(3, open);
                        pstmt.setDouble(4, high);
                        pstmt.setDouble(5, low);
                        pstmt.setDouble(6, close);
                        pstmt.setLong(7, volume);
                        pstmt.executeUpdate();
                    }
                }
            }
        }
    }

    private static void storeLastUpdateOfTheDay() throws SQLException {
        LocalDateTime now = LocalDateTime.now();
        LocalDate date = LocalDate.now();
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
            String sql = "INSERT INTO nepse_data (date, symbol, open, high, low, close, volume) " +
                    "SELECT ?, symbol, open, high, low, close, volume FROM nepse_data " +
                    "WHERE date = (SELECT MAX(date) FROM nepse_data) " +
                    "ON DUPLICATE KEY UPDATE close = VALUES(close), volume = VALUES(volume)";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setObject(1, date.atTime(END_OF_DAY));
                pstmt.executeUpdate();
            }
        }
    }
}
