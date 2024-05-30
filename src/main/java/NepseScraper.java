import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.io.IOException;
import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Random;

public class NepseScraper {

    private static final String DB_URL = "jdbc:mysql://localhost:3306/nepse_data";
    private static final String DB_USER = "root";
    private static final String DB_PASS = "";

    private static final String[] USER_AGENTS = {
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/117.0",
    };

    public static void main(String[] args) throws IOException, SQLException {
        String historicalDataURL = "https://nepsealpha.com";
        String currentDataURL = "https://www.sharesansar.com/today-share-price";

        createTableIfNotExists();

//        scrapeAndStoreData(fetchDocument(historicalDataURL), true);
        scrapeAndStoreData(fetchDocument(currentDataURL), false);
    }

    private static Document fetchDocument(String url) throws IOException {
        Random random = new Random();
        String userAgent = USER_AGENTS[random.nextInt(USER_AGENTS.length)];
        return Jsoup.connect(url).userAgent(userAgent).get();
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

    private static void scrapeAndStoreData(Document doc, boolean isHistorical) throws SQLException {
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
            for (Element row : doc.select("table.table tr")) {
                Elements cells = row.select("td");
                if (cells.size() >= 7) {
                    String dateStr = cells.get(0).text();

                    LocalDate date;
                    if (isHistorical) {
                        date = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                    } else {
                        date = LocalDate.now();
                    }

                    String symbol = cells.get(1).text();
                    double open = Double.parseDouble(cells.get(2).text().replace(",", ""));
                    double high = Double.parseDouble(cells.get(3).text().replace(",", ""));
                    double low = Double.parseDouble(cells.get(4).text().replace(",", ""));
                    double close = Double.parseDouble(cells.get(5).text().replace(",", ""));
                    double volumeDouble = Double.parseDouble(cells.get(6).text().replace(",", ""));
                    long volume = (long) volumeDouble;

                    String sql = "INSERT INTO nepse_data (date, symbol, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE open = ?, high = ?, low = ?, close = ?, volume = ?";

                    try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                        pstmt.setObject(1, date);
                        pstmt.setString(2, symbol);
                        pstmt.setDouble(3, open);
                        pstmt.setDouble(4, high);
                        pstmt.setDouble(5, low);
                        pstmt.setDouble(6, close);
                        pstmt.setLong(7, volume);
                        pstmt.setDouble(8, open); // For ON DUPLICATE KEY UPDATE
                        pstmt.setDouble(9, high);
                        pstmt.setDouble(10, low);
                        pstmt.setDouble(11, close);
                        pstmt.setLong(12, volume);
                        pstmt.executeUpdate();
                    }
                }
            }
        }
    }
}
