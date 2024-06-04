import org.openqa.selenium.chrome.ChromeDriver;

public class AutomationSelen {

    public static void main(String[] args) {
        System.setProperty("webdriver.chrome.driver","D:\\downloads\\chromedriver.exe");

        ChromeDriver driver = new ChromeDriver();
        driver.get("http://www.google.com");
    }
}
