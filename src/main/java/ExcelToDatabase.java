import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

public class ExcelToDatabase {
    private static final String JDBC_URL = "jdbc:mysql://localhost:3306/nepse_demodata";
    private static final String USERNAME = "root"; // Replace with your MySQL username
    private static final String PASSWORD = ""; // Replace with your MySQL password
    private static final String FOLDER_PATH = "D:/downloads/nepsedata/"; // Replace with your actual folder path
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);

    public static void main(String[] args) {
        try (Connection connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD)) {
            createTableIfNotExists(connection);

            List<StockData> stockDataList = new ArrayList<>();
            Files.list(Path.of(FOLDER_PATH))
                    .filter(path -> path.getFileName().toString().toLowerCase().endsWith(".xlsx"))
                    .forEach(path -> processExcelFile(path, stockDataList));

            insertStockData(connection, stockDataList);
            System.out.println("Data insertion complete.");
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }

    // Method to process each Excel file
    private static void processExcelFile(Path path, List<StockData> stockDataList) {
        try (Workbook workbook = new XSSFWorkbook(new FileInputStream(path.toFile()))) {
            Sheet sheet = workbook.getSheetAt(0);
            Iterator<Row> rowIterator = sheet.iterator();
            rowIterator.next(); // Skip header row

            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                try {
                    stockDataList.add(parseStockData(row));
                } catch (DateTimeParseException | NumberFormatException e) {
                    System.err.println("Error parsing row: " + row.getRowNum() + " - " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading Excel file: " + path.getFileName());
            e.printStackTrace();
        }
    }

    // StockData class (inner class)
    static class StockData {
        LocalDate date;
        String symbol;
        double open;
        double high;
        double low;
        double close;
        double turnover;  // Changed to double
        double volume;

        public StockData(LocalDate date, String symbol, double open, double high, double low, double close, double turnover, double volume) {
            this.date = date;
            this.symbol = symbol;
            this.open = open;
            this.high = high;
            this.low = low;
            this.close = close;
            this.turnover = turnover;
            this.volume = volume;
        }
    }

    // Method to create the table if it doesn't exist
    private static void createTableIfNotExists(Connection connection) throws SQLException {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS stock_data (" +
                "id INT AUTO_INCREMENT PRIMARY KEY," +
                "date DATE," +
                "symbol VARCHAR(10)," +
                "open DECIMAL(10, 2)," +
                "high DECIMAL(10, 2)," +
                "low DECIMAL(10, 2)," +
                "close DECIMAL(10, 2)," +
                "turnover DECIMAL(20, 2)," + // Changed to DECIMAL for turnover
                "volume DECIMAL(20, 2)" +
                ")";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSQL);
        }
    }

    // Method to parse stock data from a row
    private static StockData parseStockData(Row row) throws DateTimeParseException, NumberFormatException {
        String dateStr = getCellValue(row.getCell(1));
        LocalDate date = LocalDate.parse(dateStr, DATE_FORMATTER);
        String symbol = getCellValue(row.getCell(3));
        double open = Double.parseDouble(getCellValue(row.getCell(5)));
        double high = Double.parseDouble(getCellValue(row.getCell(6)));
        double low = Double.parseDouble(getCellValue(row.getCell(7)));
        double close = Double.parseDouble(getCellValue(row.getCell(8)));
        double turnover = Double.parseDouble(getCellValue(row.getCell(9))); // Parse turnover as double
        double volume = Double.parseDouble(getCellValue(row.getCell(10)));
        return new StockData(date, symbol, open, high, low, close, turnover, volume);
    }

    // Method to get cell value as string
    private static String getCellValue(Cell cell) {
        if (cell == null) {
            return "";
        }
        switch (cell.getCellType()) {
            case STRING:
                return cell.getStringCellValue();
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    return cell.getDateCellValue().toString();
                } else {
                    return String.valueOf(cell.getNumericCellValue());
                }
            case BOOLEAN:
                return String.valueOf(cell.getBooleanCellValue());
            case FORMULA:
                return cell.getCellFormula();
            default:
                return "";
        }
    }

    // Method to insert stock data into the database in batches
    private static void insertStockData(Connection connection, List<StockData> stockDataList) throws SQLException {
        String sql = "INSERT INTO stock_data (date, symbol, open, high, low, close, turnover, volume) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (StockData stockData : stockDataList) {
                statement.setDate(1, Date.valueOf(stockData.date));
                statement.setString(2, stockData.symbol);
                statement.setDouble(3, stockData.open);
                statement.setDouble(4, stockData.high);
                statement.setDouble(5, stockData.low);
                statement.setDouble(6, stockData.close);
                statement.setDouble(7, stockData.turnover); // Set turnover as double
                statement.setDouble(8, stockData.volume);
                statement.addBatch();
            }

            statement.executeBatch();
        }
    }
}
