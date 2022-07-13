import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;


public class Node implements Runnable{

    private DatagramSocket s, node2, node3;
    private static Thread mainThread, stdReadThread, htbThread;
    private String ip_address = "127.0.0.1";
    private String server_ip = "127.0.0.1";
    private final static Logger fLogger = Logger.getLogger(Node.class.getName());
    byte[] buf = new byte[1000];
    int bootstrapServerPort = 55555;
    int nodePort = 5001;
    String nodeName = "Node";
    int maxHops = 3;
    String portfileName = "Port";
    InetAddress hostAddress;
    DatagramPacket dp;
    HashMap<String, File> filesToStore = new HashMap<String, File>();
    HashMap<String, String> addressHistory = new HashMap<String, String>();
    List<Neighbour> joinedNodes = new ArrayList<Neighbour>();
    List<String> nodeFiles = new ArrayList<String>();
    static Thread joinThread;


    public Node() throws Exception {
        s = new DatagramSocket();
        InetAddress IP = InetAddress.getLocalHost();
        echo("IP address: " + ip_address);
        hostAddress = InetAddress.getByName(server_ip);
        try {
            dp = new DatagramPacket(buf, buf.length);
            setFiles();
        } catch (Exception e) {

        }
    }

    public static void main(String args[]) throws Exception {

        Node n1 = new Node();
        mainThread = new Thread(n1);
        stdReadThread = new Thread(() -> {
            n1.getCommandLineOutput();
        });

        Thread listnerThread = new Thread(() -> {
            n1.joinListener();
        });

        htbThread = new Thread(() -> {
            System.out.println("sending heart beat....");
//            try {
//                n1.sendHbt();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                n1.unreg();
            }
        });

        mainThread.start();
        stdReadThread.start();
        listnerThread.start();
        htbThread.start();
    }
    public void setServer(String server) {
        server_ip = server;
    }
    public void setName(String name) {
        nodeName = name;
    }

    public void setIP(String ip) {
        ip_address = ip;
    }

    public void setPort(int port) {
        nodePort = port;
    }

    public int getPort() {
        return nodePort;
    }

    public void echo(String msg) {
        System.out.println(msg);
    }

    public void searchFile(String fileName) {

    }

    public void serialize() {
        try (OutputStream file = new FileOutputStream("addresses.ser");
                OutputStream buffer = new BufferedOutputStream(file);
                ObjectOutput output = new ObjectOutputStream(buffer);) {
            output.writeObject(addressHistory);
        } catch (IOException ex) {
            fLogger.log(Level.SEVERE, "Cannot perform output.", ex);
        }
    }

    public void deserialize() {
        try (InputStream file = new FileInputStream("addresses.ser");
                InputStream buffer = new BufferedInputStream(file);
                ObjectInput input = new ObjectInputStream(buffer);) {
            List<String> recoveredQuarks = (List<String>) input.readObject();
            for (String quark : recoveredQuarks) {
                System.out.println("Recovered Quark: " + quark);
            }
        } catch (ClassNotFoundException e) {
            fLogger.log(Level.SEVERE, "Cannot perform input. Class not found.", e);
        } catch (FileNotFoundException e) {
            fLogger.log(Level.SEVERE, "Cannot perform input. File not found.", e);
        } catch (IOException e) {
            fLogger.log(Level.SEVERE, "Cannot perform input. IO exception.", e);
        }
    }

    //Randomly pick two files from the file list.
    public void setFiles() {

        HashMap<String, File> films = new HashMap<String, File>();

        films.put("Harry_Potter", new File("D:\\Films\\Lord_of_the_Rings.mov"));
        films.put("The_Seven_Samurai", new File("D:\\Films\\Harry_Porter_1.mov"));
        films.put("Fast_and_Furious", new File("D:\\Films\\Fast_and_Furious.mov"));
        films.put("Bonnie_and_Clyde", new File("D:\\Films\\La_La_Land.mov"));
        films.put("Pan's_Labyrinth", new File("D:\\Films\\Transformers.mov"));
        films.put("Doctor_Zhivago", new File("D:\\Films\\Transformers.mov"));
        films.put("Spider_Man_", new File("D:\\Films\\Spider_Man_1.mov"));
        films.put("Reservoir_Dogs\n", new File("D:\\Films\\abc.mov"));
        films.put("Airplane!", new File("D:\\Films\\Lord_of_the_Rings.mov"));

        //generate 3 random indices to pick files from hashmap
        int[] randomIndices = new Random().ints(1, films.size()).distinct().limit(3).toArray();

        //pick files randomly
        ArrayList<String> keysAsArray = new ArrayList<String>(films.keySet());
        for (int fileIndex : randomIndices) {
            filesToStore.put(keysAsArray.get(fileIndex), films.get(keysAsArray.get(fileIndex)));
            System.out.println(keysAsArray.get(fileIndex));
        }

    }

    public void initializeSocket(int port) { // initiating the listening for the port
        echo("listening to " + port);
        try {
            node2 = new DatagramSocket(port);
        } catch (Exception e) {

        }
    }

    public void joinListener() {
        byte[] buffer = new byte[65536];
        DatagramPacket incoming = new DatagramPacket(buffer, buffer.length);
        initializeSocket(nodePort);
        while (true) {

            try {
                node2.receive(incoming);
            } catch (Exception e) {

            }
            byte[] data = incoming.getData();
            String str = new String(data, 0, incoming.getLength());

            if(!str.contains("hbt") && !str.contains("hbtok"))
            echo(incoming.getAddress().getHostAddress() + " : " + incoming.getPort() + " - " + str);

            StringTokenizer st = new StringTokenizer(str, " ");
            String command = "", length = "";
            String ip = incoming.getAddress().getHostAddress();
            int port = incoming.getPort();

            try {
                length = st.nextToken();
                command = st.nextToken();
                echo("command: " + command);
                System.out.println("Commands comming...");

                if (command.equals("JOIN")) {
                    String neighbour_ip = st.nextToken();
                    String neighbour_port =  st.nextToken();
                    String reply = " JOINOK "+ip_address+" "+ nodePort;
                    reply = "00" + (reply.length() + 2) + reply;
                    sendMessage(reply, ip, neighbour_port);
                    Neighbour tempNeighbour = new Neighbour(ip,Integer.parseInt(neighbour_port), "neighbour");
                    joinedNodes.add(tempNeighbour);

                } else if (command.equals("JOINOK")) {
                    String neighbour_ip = st.nextToken();
                    String neighbour_port =  st.nextToken();
                    Neighbour tempNeighbour = new Neighbour(ip, Integer.parseInt(neighbour_port), "neighbour");
                    joinedNodes.add(tempNeighbour);
                    echo(Integer.toString(joinedNodes.size()));
                } else if (command.equals("hbt")) {

                    String originatorIP = st.nextToken();
                    int originatorPort = Integer.parseInt(st.nextToken());

                    String reg = " hbtok "; //send this node values to other
                    reg = "00" + (reg.length() + 4) + reg;

                    sendMessage_no_stdout(reg, originatorIP, Integer.toString(originatorPort));

                } else if (command.equals("hbtok")) {

                } else if (command.equals("SER")) {

                    String originatorIP = st.nextToken();
                    int originatorPort = Integer.parseInt(st.nextToken());
                    String searchFile = st.nextToken();
                    int hops = Integer.parseInt(st.nextToken());
                    System.out.println("SER " + originatorIP + " " + searchFile + " " + hops);

                    if (hops < 0) {
                        String searchResultNotFoundCommand = " SEROK 0 " + ip_address + " " + nodePort + " "
                                + (maxHops - hops);
                        searchResultNotFoundCommand = "00" + (searchResultNotFoundCommand.length() + 4)
                                + searchResultNotFoundCommand;

                        sendMessage(searchResultNotFoundCommand, originatorIP, String.valueOf(originatorPort));

                    } else {
                        int totalResults = 0;
                        ArrayList<String> searchResults = new ArrayList<String>();

                        for (String fileNames : filesToStore.keySet()) {
                            System.out.println(fileNames+" "+searchFile);
                            if (fileNames.contains(searchFile)) {
                                totalResults++;
                                searchResults.add(fileNames);
                            }
                        }
                        if(totalResults > 0) {
                            --hops;
                            String searchResultOkCommand = " SEROK " + totalResults + " " + ip_address + " " + nodePort
                                    + " " + (maxHops - hops);
                            for (String fileName : searchResults) {
                                searchResultOkCommand += " " + fileName;
                            }

                            if (searchResultOkCommand.length() < 96) {
                                searchResultOkCommand = "00" + (searchResultOkCommand.length() + 4)
                                        + searchResultOkCommand;
                            } else {
                                searchResultOkCommand = "0" + (searchResultOkCommand.length() + 4)
                                        + searchResultOkCommand;
                            }
                            sendMessage(searchResultOkCommand, originatorIP, String.valueOf(originatorPort));

                        } else if (totalResults == 0) {
                            //select random node from neighbours
                            Random r = new Random();
                            Neighbour randomSuccessor = null;

                            while (true) {
                                randomSuccessor = joinedNodes.get(r.nextInt(joinedNodes.size()));

                                if (!(randomSuccessor.getIp().equals(incoming.getAddress().getHostAddress())
                                        && randomSuccessor.getPort() == incoming.getPort())) {
                                    break;
                                }
                            }
                            String searchCommand = " SER " + originatorIP + " " + originatorPort + " "+searchFile+" "
                                    + --hops;

                            searchCommand = "00" + (searchCommand.length() + 4) + searchCommand;
                            sendMessage(searchCommand, randomSuccessor.getIp(),
                                    String.valueOf(randomSuccessor.getPort()));

                            System.out.println("Request is forwareded!!!");
                        }
                    }
                } else if (command.equals("SEROK")) {
                    int totalResults = Integer.parseInt(st.nextToken());
                    String respondedNodeIP = st.nextToken();
                    int respondedNodePort = Integer.parseInt(st.nextToken());
                    int hops = Integer.parseInt(st.nextToken());

                    System.out.println("Responded Node IP: " + respondedNodeIP);
                    System.out.println("Responded Node Port: " + respondedNodePort);
                    System.out.println("Total No. of Results: " + totalResults);
                    System.out.println("No of Hops request went through: " + hops);
                    for (int i = 0; i < totalResults; i++) {
                        System.out.println(st.nextToken());
                    }
                } else if (command.equals("REGOK")) {

                    int no_nodes = Integer.parseInt(st.nextToken());

                    System.out.println(" ### Send Join Request for Neighboring Nodes: " + no_nodes);

                    while (no_nodes > 0 && no_nodes < 21) {

                        String join_ip = st.nextToken();
                        String join_port = st.nextToken();

                        if (Integer.parseInt(join_port) != nodePort) {
                            String join = " JOIN " + join_ip + " " + join_port;
                            String join_msg = "00" + (join.length() + 4) + join;

                            sendJoinReq(join_msg, join_ip, Integer.parseInt(join_port));
                            no_nodes -= 1;
                        }
                    }

                }
            } catch (Exception e) {
                System.out.println(e);
                for (int i = 0; i < 3; i++) {
                    initializeSocket(nodePort);
                }
            }
        }

    }

    public void sendJoinReq(String outString, String outAddress, int outPort) {
        try {
            buf = outString.getBytes();
            DatagramPacket out = new DatagramPacket(buf, buf.length, InetAddress.getByName(outAddress), outPort);

            System.out.println("SENDING... => " + outString + " to " + outPort);
            node2.send(out);
        } catch (Exception e) {

        }
    }

    public void listener() {

        byte[] buffer = new byte[65536];
        DatagramPacket incoming = new DatagramPacket(buffer, buffer.length);
        try {
            s.receive(incoming);
        } catch (Exception e) {

        }

        byte[] data = incoming.getData();
        String str = new String(data, 0, incoming.getLength());

        if(!str.contains("hbt") && !str.contains("hbt"))
        echo(incoming.getAddress().getHostAddress() + " : " + incoming.getPort() + " - " + str);

        StringTokenizer st = new StringTokenizer(str, " ");
        String length = "", command = "";
        try {
            length = st.nextToken();
            command = st.nextToken();
        } catch (Exception e) {

        }

        if (command.equals("REGOK")) {

            int no_nodes = Integer.parseInt(st.nextToken());

            while (no_nodes > 0 && no_nodes < 21) {

                String join_ip = st.nextToken();
                String join_port = st.nextToken();

                if (Integer.parseInt(join_port) != nodePort) {
                    String join = " JOIN " + join_ip + " " + join_port;
                    String join_msg = "00" + (join.length() + 4) + join;

                    sendJoinReq(join_msg, join_ip, Integer.parseInt(join_port));
                    no_nodes -= 1;
                }
            }

            if (no_nodes == 9999) {
                echo("There's an error in the command");
            } else if (no_nodes == 9998) {
                unreg();
                doREG();
            }

        }
        else if (command.equals("JOIN")) {
            echo("JOINED");
        }
    }

    public void run() {
        try {
            startNode();
        } catch (Exception e) {
            echo("Cannot start node!");
        }
    }

    public void getCommandLineOutput() {
        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            try {
                String outMessage = stdin.readLine();
                System.out.println("###### Command line input received : " + outMessage);
                if (outMessage.equals("bye")) {

                    System.exit(1);
                } else if (outMessage.equals("join")) {

                    sendMessage("test from n1", "127.0.1.1", "5001");

                } else if (outMessage.contains("ser")) {
                    String searchQuery = outMessage.split(" ")[1];

                    int totalResults = 0;
                    ArrayList<String> searchResults = new ArrayList<String>();

                    for (String fileNames : filesToStore.keySet()) {
                        if (fileNames.contains(searchQuery)) {
                            totalResults++;
                            searchResults.add(fileNames);
                        }
                    }
                    if(totalResults == 0) {
                        //select random node from neighbours
                        System.out.println("############ No results were found from my node looking in neighbour nodes");
                        Random r = new Random();
                        Neighbour randomSuccessor = joinedNodes.get(r.nextInt(joinedNodes.size()));

                        //send search message to picked neighbour
                        String searchCommand = " SER " + ip_address + " " + nodePort + " " + searchQuery + " " + maxHops;
                        searchCommand = "00" + (searchCommand.length() + 4) + searchCommand;
                        sendMessage(searchCommand, randomSuccessor.getIp(), String.valueOf(randomSuccessor.getPort()));
                    } else {
                        System.out.println("########### File found in my node");
                    }

                } else if (outMessage.contains("nodes")) {
                    for(int i=0;i<joinedNodes.size();i++){
                        System.out.println(joinedNodes.get(i));
                    }

                } else {
                    echo("Enter valid command");
                }
            }
            catch (Exception e) {

            }
        }

    }

    public void startNode() throws Exception {
        try {
            doREG();
        }
        catch (Exception e) {
            echo("IO Exception");
        }
    }

    public void writePort(int port) {
        try {
            FileWriter fileWriter = new FileWriter(portfileName);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            bufferedWriter.write(Integer.toString(port));
            bufferedWriter.close();
        } catch (IOException ex) {
            System.out.println("Error writing to file '" + portfileName + "'");
        }
    }

    public void doREG() {
        Random r = new Random();
        nodePort = Math.abs(r.nextInt()) % 6000 + 3000;
        //writePort(nodePort);
        String reg = " REG " + ip_address + " " + nodePort + " " + nodeName;
        reg = "00" + (reg.length() + 4) + reg;

        sendMessage(reg, server_ip, Integer.toString(bootstrapServerPort));
    }

    public void unreg() {
        String reg = " UNREG " + ip_address + " " + nodePort + " " + nodeName;
        reg = "00" + (reg.length() + 4) + reg;
        sendMessage(reg, server_ip, Integer.toString(bootstrapServerPort));
    }

    public void unregPort(int port) {
        String reg = " UNREG " + ip_address + " " + port + " " + nodeName;
        reg = "00" + (reg.length() + 4) + reg;
        sendMessage(reg, server_ip, Integer.toString(bootstrapServerPort));
    }

    public void sendMessage(String outString, String outAddress, String outPort) {
        try {

            buf = outString.getBytes();
            DatagramPacket out = new DatagramPacket(buf, buf.length, InetAddress.getByName(outAddress),
                    Integer.parseInt(outPort));

                System.out.println("SENDING... => " + outString + " to " + outPort);
            s.send(out);
        } catch (Exception e) {
            echo("Send error!");
        }
    }

    public void sendMessage_no_stdout(String outString, String outAddress, String outPort) {
        try {

            buf = outString.getBytes();
            DatagramPacket out = new DatagramPacket(buf, buf.length, InetAddress.getByName(outAddress),
                    Integer.parseInt(outPort));
            s.send(out);
        } catch (Exception e) {
            echo("Send error!");
        }
    }

    public void sendHbt() throws InterruptedException {
        while (true) {
            for (Neighbour n : joinedNodes) {
                String reg = " hbt " + ip_address + " " + nodePort; //send this node values to other
                reg = "00" + (reg.length() + 4) + reg;
                System.out.println("#### MY PORT: " + nodePort);
                System.out.println("#### Heart Beat sent to " + n.getPort());
                sendMessage_no_stdout(reg, n.getIp(), Integer.toString(n.getPort()));
            }
            Thread.sleep(5000);
        }

    }
}