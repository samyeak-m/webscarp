import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class DynamicClassRunner {
    public static void main(String[] args) {
        // Specify the directory where your classes are located
        String directory = "D:/samyeak/BCA/6thsem/project/webscrap/src/main/java";

        // Scan the directory for .class files
        File folder = new File(directory);
        File[] files = folder.listFiles((dir, name) -> name.endsWith(".java"));

        // Collect class names without extension
        List<String> classNames = new ArrayList<>();
        for (File file : files) {
            String fileName = file.getName();
            classNames.add(fileName.substring(0, fileName.lastIndexOf('.')));
        }

        // Ask for user input
        Scanner scanner = new Scanner(System.in);
        List<String> selectedClasses = new ArrayList<>();

        System.out.println("Select classes to run (comma-separated):");
        for (int i = 0; i < classNames.size(); i++) {
            System.out.println((i + 1) + ". " + classNames.get(i));
        }

        String input = scanner.nextLine();
        String[] selections = input.split(",");
        for (String selection : selections) {
            int index = Integer.parseInt(selection.trim()) - 1;
            if (index >= 0 && index < classNames.size()) {
                selectedClasses.add(classNames.get(index));
            } else {
                System.out.println("Invalid selection: " + selection);
            }
        }

        System.out.println("Selected classes: " + selectedClasses);

        // Run selected classes
        for (String className : selectedClasses) {
            runClass(className);
        }

        scanner.close();
    }

    private static void runClass(String className) {
        try {
            // Dynamically load the class
            Class<?> clazz = Class.forName(className);
            // Get the main method of the class
            Method main = clazz.getMethod("main", String[].class);
            // Invoke the main method with null as argument (no command line args)
            main.invoke(null, (Object) new String[]{});
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
