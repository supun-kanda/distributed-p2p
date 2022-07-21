import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;

import static config.Constant.DOWNLOAD_STORE;

/**
 * @author Supun Kandambige
 * Date: 21/07/2022
 */
public class Client {
    private DataInputStream dis;
    private DataOutputStream dos;
    private BufferedReader br;
    private String ip;
    private int port;

    public Client(String ip, int port) {
        this.ip = ip;
        this.port = port;
        InputStreamReader isr = new InputStreamReader(System.in);
        try {
            Socket client = new Socket(ip, port);
            dis = new DataInputStream(client.getInputStream());
            dos = new DataOutputStream(client.getOutputStream());
        } catch (IOException e) {
            System.err.println("Error while connecting to the node: " + e);
        }
    }

    public void receiveFile(String fileName) {
        try {
            int bytesRead;
            System.out.println("Downloading the file: " + fileName);

            dos.writeUTF("DOWNLOAD_FILE");
            dos.writeUTF(fileName);

            DataInputStream serverData = dis;

            String fileNameAck = serverData.readUTF(); // read filename
            OutputStream output = new FileOutputStream(DOWNLOAD_STORE + fileNameAck);
            long size = serverData.readLong(); // read size
            byte[] buffer = new byte[1024];

            while (size > 0 &&
                    (bytesRead = serverData.read(buffer, 0, (int) Math.min(buffer.length, size))) != -1) { // read data
                output.write(buffer, 0, bytesRead);
                size -= bytesRead;
            }

            output.close();
            serverData.close();
        } catch (Exception e) {
            System.err.println("Error while receiving the file: " + fileName + ": " + e);
        }
    }

    @Override
    public String toString() {
        return "Client{" +
                "ip='" + ip + '\'' +
                ", port=" + port +
                '}';
    }
}
