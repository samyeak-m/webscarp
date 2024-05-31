import javax.swing.*;
import java.awt.*;
import java.awt.event.*;

class WindowExample {
    JFrame f1;
    public void setWindow(){
        f1 = new JFrame();
        f1.setVisible(true);
        f1.setSize(500,500);
        f1.setLayout(new FlowLayout());
        f1.setDefaultCloseOperation(3);

        f1.addWindowListener(new WindowListener() {
            @Override
            public void windowOpened(WindowEvent e) {
//                when window is open and start working
                System.out.println("window working");

            }

            @Override
            public void windowClosing(WindowEvent e) {
//                when window is closed using menu bar

                System.out.println("window is closing");
            }

            @Override
            public void windowClosed(WindowEvent e) {
//                when window is exceptionally closed

                System.out.println("window closed");
            }

            @Override
            public void windowIconified(WindowEvent e) {
//                when window is minimized

                System.out.println("window minimized (iconified)");
            }

            @Override
            public void windowDeiconified(WindowEvent e) {
//                when window is back to active after minimize

                System.out.println("window back to normal"+"after minimize");
            }

            @Override
            public void windowActivated(WindowEvent e) {
//                when window is in working

                System.out.println("window active");
            }

            @Override
            public void windowDeactivated(WindowEvent e) {
//                when winodw is not working

                System.out.println("window deactivated");
            }
        });
    }


}

public class WindowEventDemo {
    public static void main(String[] args) {
        WindowExample w1 = new WindowExample();
        w1.setWindow();
    }



}
