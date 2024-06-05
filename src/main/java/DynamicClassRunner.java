import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class DynamicClassRunner {
    public static void main(String[] args) {
        String directory = "D:/samyeak/BCA/6thsem/project/webscrap/src/main/java";

        File folder = new File(directory);
        File[] files = folder.listFiles((dir, name) -> name.endsWith(".java"));

        List<String> classNames = new ArrayList<>();
        for (File file : files) {
            String fileName = file.getName();
            classNames.add(fileName.substring(0, fileName.lastIndexOf('.')));
        }

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

        for (String className : selectedClasses) {
            runClass(className);
        }

        scanner.close();
    }

    private static void runClass(String className) {
        try {
            Class<?> clazz = Class.forName(className);
            Method main = clazz.getMethod("main", String[].class);
            main.invoke(null, (Object) new String[]{});
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
