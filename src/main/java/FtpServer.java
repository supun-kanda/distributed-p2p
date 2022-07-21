import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.net.ServerSocket;
import java.net.Socket;

import static config.Constant.FILE_STORE;
import static config.Constant.TCP_PORT;

/**
 * @author Supun Kandambige
 * Date: 21/07/2022
 */
class Server {
    private final int port;

    public Server() {
        this.port = TCP_PORT;
    }

    public Server(int port) {
        this.port = port;
    }

//    public static void main(String[] arg) {
//        Server s = new Server();
//        s.doConnections();
//    }

    public void doConnections() {

        try {
            ServerSocket server = new ServerSocket(port);
            System.out.println("Server started on: " + port);
            while (true) {
                Socket client = server.accept();
                ClientThread ct = new ClientThread(client);
                ct.start();
            }
        } catch (Exception e) {
            System.err.println("Error occurred while initiating: " + e);
        }
    }
}

class ClientThread extends Thread {
    private DataInputStream dis;
    private final Socket socket;

    public ClientThread(Socket socket) {
        this.socket = socket;
        try {
            dis = new DataInputStream(socket.getInputStream());
        } catch (Exception e) {
            System.err.println("Error occurred while initiating: " + e);
        }
    }

    public void run() {
        while (true) {
            try {
                if (dis.available() == 0) {
                    continue;
                }
                String input = dis.readUTF();

                if (input.equals("DOWNLOAD_FILE")) {
                    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());

                    String fileName = dis.readUTF();
                    System.out.println("Requesting to download: " + fileName);
                    File file = new File(FILE_STORE + fileName);
                    if (file.isFile()) {

                        byte[] mybytearray = new byte[(int) file.length()];

                        FileInputStream fis = new FileInputStream(file);
                        BufferedInputStream bis = new BufferedInputStream(fis);
                        DataInputStream dis = new DataInputStream(bis);
                        dis.readFully(mybytearray, 0, mybytearray.length);


                        dos.writeUTF(fileName); // write name
                        dos.writeLong(mybytearray.length); // write length
                        dos.write(mybytearray, 0, mybytearray.length); // write data

                        dos.flush();
                        fis.close();
                    } else {
                        dos.writeUTF(""); // NO FILE FOUND
                        System.err.println("No file found");
                    }
                } else {
                    System.out.println("Error at Server invalid input: " + input);
                }
            } catch (Exception e) {
                System.err.println("Error at Server: " + e);
            }
        }
    }
}
