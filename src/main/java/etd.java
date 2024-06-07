import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

public class etd {
    private static final String JDBC_URL = "jdbc:mysql://localhost:3306/nepse_data";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "";
    private static final String FOLDER_PATH = "D:/downloads/Historicnepse/";
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);

    public static void main(String[] args) {
        try (Connection connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD)) {
            createTableIfNotExists(connection);

            List<ExcelToDatabase.StockData> stockDataList = new ArrayList<>();
            Files.list(Path.of(FOLDER_PATH))
                    .filter(path -> path.getFileName().toString().toLowerCase().endsWith(".xlsx"))
                    .forEach(path -> processExcelFile(path, stockDataList));

            insertStockData(connection, stockDataList);
            System.out.println("Data insertion complete.");
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }

    private static void processExcelFile(Path path, List<ExcelToDatabase.StockData> stockDataList) {
        try (Workbook workbook = new XSSFWorkbook(new FileInputStream(path.toFile()))) {
            Sheet sheet = workbook.getSheetAt(0);
            Iterator<Row> rowIterator = sheet.iterator();
            rowIterator.next();

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

    static class StockData {
        LocalDate date;
        String symbol;
        double open;
        double high;
        double low;
        double close;
        double turnover;
        double vol;

        public StockData(LocalDate date, String symbol, double open, double high, double low, double close, double turnover, double vol) {
            this.date = date;
            this.symbol = symbol;
            this.open = open;
            this.high = high;
            this.low = low;
            this.close = close;
            this.turnover = turnover;
            this.vol = vol;
        }
    }

    private static void createTableIfNotExists(Connection connection) throws SQLException {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS histock_data (" +
                "id INT AUTO_INCREMENT PRIMARY KEY," +
                "date DATE," +
                "symbol VARCHAR(50)," +
                "open DECIMAL(10, 2)," +
                "high DECIMAL(10, 2)," +
                "low DECIMAL(10, 2)," +
                "close DECIMAL(10, 2)," +
                "turnover DECIMAL(20, 2)," +
                "vol DECIMAL(20, 2)" +
                ")";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSQL);
        }
    }

    private static ExcelToDatabase.StockData parseStockData(Row row) throws DateTimeParseException, NumberFormatException {
        String dateStr = getCellValue(row.getCell(19));
        LocalDate date = LocalDate.parse(dateStr, DATE_FORMATTER);
        String symbol = getCellValue(row.getCell(1));
        double open = Double.parseDouble(getCellValue(row.getCell(3)));
        double high = Double.parseDouble(getCellValue(row.getCell(4)));
        double low = Double.parseDouble(getCellValue(row.getCell(5)));
        double close = Double.parseDouble(getCellValue(row.getCell(6)));
        double turnover = Double.parseDouble(getCellValue(row.getCell(10)));
        double vol = Double.parseDouble(getCellValue(row.getCell(8)));
        return new ExcelToDatabase.StockData(date, symbol, open, high, low, close, turnover, vol);
    }

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


    private static void insertStockData(Connection connection, List<ExcelToDatabase.StockData> stockDataList) throws SQLException {
        String sql = "INSERT INTO histock_data (date, symbol, open, high, low, close, turnover, vol) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (ExcelToDatabase.StockData stockData : stockDataList) {
                statement.setDate(1, Date.valueOf(stockData.date));
                statement.setString(2, stockData.symbol);
                statement.setDouble(3, stockData.open);
                statement.setDouble(4, stockData.high);
                statement.setDouble(5, stockData.low);
                statement.setDouble(6, stockData.close);
                statement.setDouble(7, stockData.turnover);
                statement.setDouble(8, stockData.vol);
                statement.addBatch();
            }

            statement.executeBatch();
        }
    }
}
