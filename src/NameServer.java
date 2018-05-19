import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;

public class NameServer {

    /* ******* MAIN ******* */

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Error : Name Server requires config file argument");
            System.exit(0);
        }
        NameServer nameServer = new NameServer(args[0]);
    }

    /* ******* FIELDS ******* */

    protected static final int HASH_SIZE = 1024;

    protected static final String REGISTER = "register";
    protected static final String EXIT = "exit";
    protected static final String REMOVE_SELF = "remove_self";
    protected static final String LOOKUP = "lookup";
    protected static final String PRINT = "print";
    protected static final String DELETE = "delete";
    protected static final String INSERT = "insert";
    protected static final String SET_PREV = "set_prev";
    protected static final String SET_NEXT = "set_next";
    protected static final String TRANSFER = "transfer";
    protected static final String TRANSFER_REQUEST = "transfer_request";

    protected NameServerStub prevServer;
    protected NameServerStub nextServer;

    protected int id;
    protected int endIndex;
    protected String ip;
    protected int listenPort;

    protected String bootIp;
    protected int bootPort;

    protected String prompt;

    // Probably not the final implementation, just for basic use
    protected String[] map = new String[1024];
    protected final Object mapMutex = new Object();
    protected final Object nextMutex = new Object();
    protected final Object prevMutex = new Object();

    /* ******* CONSTRUCTORS ******* */

    public NameServer() {}

    public NameServer(String configFile) {
        parseConfigFile(configFile);
        //
        System.out.println("ID: " + id);
        System.out.println("Port: " + listenPort);
        // System.out.println(bootIp);
        // System.out.println(bootPort + "\n");

        prompt = "NameServer > ";

        nextServer = new NameServerStub(ip, listenPort, id);
        prevServer = new NameServerStub(ip, listenPort, id);

        UserThread user = new UserThread();
        user.start();

        ListenerThread listenThread = new ListenerThread();
        listenThread.start();

    }

    /* ******* METHODS ******* */

    private void parseConfigFile(String configFileName) {

        try {
            ip = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            ip = "127.0.0.1";
        }

        // Error checking
        if (configFileName == null || configFileName.length() == 0) {
            System.out.println("Error: Expected a config file.");
            System.exit(0);
        }

        // Creates new file container for config file
        File file = new File(configFileName);

        if (!file.exists() || file.isDirectory()) {
            System.out.println("Error : Invalid config file.");
            System.exit(0);
        }

        try (Scanner config = new Scanner(file)) {

            if (config.hasNext())
                id = config.nextInt();
            else {
                System.out.println("Error : Invalid config format (Expected Bootstrap Name Server ID)");
                System.exit(0);
            }


            if (config.hasNext())
                listenPort = config.nextInt();
            else {
                System.out.println("Error : Invalid config format (Expected Bootstrap Name Server listening port)");
                System.exit(0);
            }

            config.nextLine();
            if (config.hasNext()) {
                String[] pairs = config.nextLine().split(" ");
                bootIp = pairs[0];
                try {
                    bootPort = Integer.parseInt(pairs[1]);
                } catch (NumberFormatException e) {
                    System.out.println("Error : Invalid config format (Couldn't parse Bootstrap port number)");
                    System.exit(0);
                }
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.out.println("Error : File not found");
            System.exit(0);
        }

    }

    protected void printMap() {
        for (int i = 0; i < 1024; i++) {
            synchronized (mapMutex) {
                String value = map[i];
                if (value == null)
                    continue;
                System.out.println(i + " " + value);
            }
        }
    }

    private class UserThread extends Thread implements Runnable {

        private boolean registered;

        UserThread() {}

        @Override
        public void run() {

            Scanner in = new Scanner(System.in);
            String input;

            // Until break...
            while (true) {

                System.out.print(prompt);

                input = in.nextLine();
                String[] tokens = input.split(" ");

                if (tokens.length >= 1) {

                    String command = tokens[0].toLowerCase();
                    try {

                        switch (command) {
                            case "enter":
                                enter();
                                break;
                            case "exit":
                                exit();
                                break;
                            case "status":
                                if (!registered) {
                                    System.out.println("Not in system.");
                                    break;
                                }
                                printMap();
                                System.out.println("\nCovering the range " + id + " to " + endIndex);
                                if (prevServer != null)
                                    System.out.println("Previous Name Server: " + prevServer.getId());
                                else
                                    System.out.println("prev: null");


                                if (nextServer != null)
                                    System.out.println("Next Name Server: " + nextServer.getId());
                                else
                                    System.out.println("next: null");

                                break;
                            case "":
                                break;
                            default:
                                System.out.println("Invalid command \"" + command + "\"");
                                break;
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                        System.out.println("\nError : Communication with coordinator failed.");
                    }
                }
            }
        }

        private void enter() throws IOException {
            if (registered) {
                System.out.println("Error : Already in system");
                return;
            }
            registered = true;

            String trace = Integer.toString(id);
            String message = REGISTER + " " + ip + " " + listenPort + " " + id + " " + trace;

            // System.out.println("Sending : " + message);
            sendMessage(message, bootIp, bootPort);
        }

        private void exit() throws IOException {
            if (!registered) {
                System.out.println("Error : Must be in the system to exit.");
                return;
            }
            registered = false;

            String message = EXIT + " " + id;
            // System.out.println("Sending : " + message);
            sendMessage(message, bootIp, bootPort);
            System.out.println("Successful exit");
            System.out.println("Key range [" + id + ", " + endIndex + "] handed over to Name Server " + getPrevId());
        }
    }

    protected void setNext(String targetIp, int targetPort,
                           String nextIp, int nextPort, int nextId) throws IOException {
        String message = SET_NEXT + " " + nextIp + " " + nextPort + " " + nextId;
        sendMessage(message, targetIp, targetPort);
    }

    protected void setPrev(String targetIp, int targetPort,
                           String prevIp, int prevPort, int prevId) throws IOException {
        String message = SET_PREV + " " + prevIp + " " + prevPort + " " + prevId;
        sendMessage(message, targetIp, targetPort);
    }

    protected class ListenerThread extends Thread implements Runnable {

        private ServerSocket listenerSocket;
        private boolean running = true;

        ListenerThread() {
            try {
                listenerSocket = new ServerSocket(listenPort);
            } catch (IOException e) {
                e.printStackTrace();
                running = false;
            }
        }

        @Override
        public void run() {

            while (running) {

                try {
                    Socket nameServerSock = listenerSocket.accept();
                    DataInputStream in = new DataInputStream(nameServerSock.getInputStream());

                    String message = in.readUTF();

                    nameServerSock.close();
                    in.close();

                    String[] tokens = message.split(" ");

                    if (tokens.length == 0)
                        continue;

                    // System.out.println("Received: " + message);
                    HandlerThread handlerThread = new HandlerThread(message, tokens);
                    handlerThread.start();

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    protected class HandlerThread extends Thread implements Runnable {

        private boolean error = false;

        private String message;
        private String[] args;

        private String command;
        HandlerThread(String message, String[] args) {
            command = args[0];
            this.message = message;
            this.args = args;
        }

        @Override
        public void run() {

            if (error)
                return;

            try {
                switch (command) {
                    case REGISTER:
                        register();
                        break;
                    case EXIT:
                        // System.out.println("EXIT CALLED");
                        exitSystem();
                        break;
                    case REMOVE_SELF:
                        removeSelf();
                        break;
                    case LOOKUP:
                        lookup();
                        break;
                    case PRINT:
                        print();
                        break;
                    case INSERT:
                        insert();
                        break;
                    case DELETE:
                        delete();
                        break;
                    case SET_NEXT:
                        if (args.length != 4) {
                            System.out.println("Error : Set Next needs 3 args");
                            break;
                        }
                        synchronized (nextMutex) {
                            nextServer.setIp(args[1]);
                            nextServer.setPort(Integer.parseInt(args[2]));
                            nextServer.setId(Integer.parseInt(args[3]));
                        }
                        break;

                    case SET_PREV:
                        if (args.length != 4) {
                            System.out.println("Error : Set Prev needs 3 args");
                            break;
                        }
                        synchronized (prevMutex) {
                            prevServer.setIp(args[1]);
                            prevServer.setPort(Integer.parseInt(args[2]));
                            prevServer.setId(Integer.parseInt(args[3]));
                        }
                        break;


                    case TRANSFER:
                        receiveTransfer();
                        break;
                    case TRANSFER_REQUEST:
                        transferRequest();


                    default:
                        break;

                }
            } catch (IOException e) {
                e.printStackTrace();
            }


        }

        private void register() throws IOException {

            if (args.length != 5) {
                System.out.println("Error : Register command expected 4 arguments");
                return;
            }

            String nsIp = args[1];
            int nsPort = Integer.parseInt(args[2]);
            int nsId = Integer.parseInt(args[3]);

            args[4] += "," + id;

            // System.out.println(" *** Registering Name Server " + nsId + " ***");

            // TODO: Check if ID is between this.id and nextServer.Id
            if (nsId == id) {
                System.out.println("Error : id already exists!");
                return;
            }

            // REGISTER PROTOCOL
            if (isInRange(nsId)) {

                // System.out.println("Going to register " + nsId + " between " + id + " and " + endIndex);

                // Transfer data from nsId to endIndex
                // System.out.println("Transferring data to Name Server " + nsId );
                StringBuilder message = new StringBuilder(TRANSFER + " " + endIndex + " ");

                for (int i = nsId; i != endIndex + 1; i++) {

                    if (i == HASH_SIZE) {
                        i = 0;
                        continue;
                    }

                    // Otherwise do stuff
                    synchronized (mapMutex) {
                        if (map[i] != null) {
                            message.append(i).append(",").append(map[i]);
                            if (i != endIndex) {
                                message.append(";");
                            }
                        }
                        map[i] = null;
                    }
                }
                sendMessage(message.toString(), nsIp, nsPort);


                String nsMsg = PRINT + " " + "Successful entry\n";
                nsMsg += "Managing keys on the range ["+nsId + ", " + endIndex + "]\n";
                nsMsg += "Preceded by Name Server " + id + "\n";
                nsMsg += "Succeeded by Name Server " + getNextId() + "\n";
                nsMsg += "Enter Sequence: \n";
                nsMsg += parseTrace(args[4], false);

                        // Update Name Server next and prev
                setNext(nsIp, nsPort, getNextIp(), getNextPort(), getNextId());
                setPrev(nsIp, nsPort, ip, listenPort, id);

                // Update next server's previous server
                setPrev(getNextIp(), getNextPort(), nsIp, nsPort, nsId);

                // Update this server's next server
                setNext(ip, listenPort, nsIp, nsPort, nsId);

                // Update endIndex
                endIndex = nsId - 1;
                if (endIndex == -1)
                    endIndex = HASH_SIZE - 1;

                String successMsg = PRINT + " " + "Name Server " + nsId + " successfully added to the system.";
                sendMessage(successMsg, bootIp, bootPort);

                sendMessage(nsMsg, nsIp, nsPort);

            } else {
                String message = REGISTER + " " + nsIp + " " + nsPort + " " + nsId + " " + args[4];
                sendMessage(message, getNextIp(), getNextPort());
            }
        }

        private void exitSystem() throws IOException {

            if (args.length != 2) {
                System.out.println("Error : cannot exit Name Server without id");
                return;
            }

            int nsId;
            try {
                nsId = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.out.println("Error : " + args[1] + " is not a number.");
                return;
            }

            if (nsId == getNextId()) {
                // tell NS of nsId to transfer all data to previous.
                String message = TRANSFER_REQUEST + " " + ip + " " + listenPort;
                sendMessage(message, getNextIp(), getNextPort());

                message = REMOVE_SELF + " ";
                sendMessage(message, getNextIp(), getNextPort());
            } else {
                // System.out.println("Relaying exit of " + nsId + " to next NS.");

                String message = EXIT + " " + nsId;
                sendMessage(message, getNextIp(), getNextPort());
            }
        }

        private void removeSelf() throws IOException {
            setNext(getPrevIp(), getPrevPort(), getNextIp(), getNextPort(), getNextId());
            setPrev(getNextIp(), getNextPort(), getPrevIp(), getPrevPort(), getPrevId());
        }

        private void receiveTransfer() throws IOException {
            if (args.length != 3) {
                System.out.println("Error : Transfer expects only 2 arguments");
                return;
            }

            endIndex = Integer.parseInt(args[1]);
            String data = args[2];

            String[] entries = data.split(";");
            for (int i = 0; i < entries.length; i++) {

                String[] entry = entries[i].split(",");

                if (entry.length != 2)
                    throw new IOException();

                int index = Integer.parseInt(entry[0]);
                String name = entry[1];

                synchronized (mapMutex) {
                    map[index] = name;
                }
            }
        }

        private void transferRequest() throws IOException {
            if (args.length != 3) {
                System.out.println("Error : Transfer request expects only 2 arguments");
                return;
            }

            String destIp = args[1];
            int destPort;
            try {
                destPort = Integer.parseInt(args[2]);
            } catch (NumberFormatException e) {
                System.out.println("Error : " + args[2] + "is not an integer");
                return;
            }

            StringBuilder message = new StringBuilder(TRANSFER + " " + endIndex + " ");
            for (int i = id; i != endIndex + 1; i++) {
                if (i == HASH_SIZE) {
                    i = 0;
                    continue;
                }
                synchronized (mapMutex) {
                    if (map[i] != null) {
                        message.append(i).append(",").append(map[i]);
                        if (i != endIndex) {
                            message.append(";");
                        }
                    }
                    map[i] = null;
                }
            }
            sendMessage(message.toString(), destIp, destPort);
        }

        private void lookup() throws IOException {

            if (args.length != 3) {
                System.out.println("Error : Expected a key and id trace.");
                return;
            }

            int key;
            try {
                key = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.out.println("Error : " + args[1] + "is not an integer");
                return;
            }

            // Update trace!
            args[2] += "," + id;

            if (isInRange(key)) {

                String value;
                synchronized (mapMutex) {
                    value = map[key];
                }

                String message = "";

                if (value == null) {
                    message += PRINT + " " + parseTrace(args[2], true) + "Key not found";
                } else {
                    message += PRINT + " " + parseTrace(args[2], true) + "Success! Found: \"" + value
                            + "\" on Name Server " + id;
                }

                // Send to bootstrap
                sendMessage(message, bootIp, bootPort);

            } else {
                // System.out.println("Asking next name server");
                String message = LOOKUP + " " + key + " " + args[2];
                sendMessage(message, getNextIp(), getNextPort());
            }

        }

        private void print() {

            int i = message.indexOf(' ');
            if (i == -1) {
                System.out.println("Error : Malformed print command.");
                return;
            }

            String msg = message.substring(i + 1);
            System.out.println();
            System.out.println(msg);
            System.out.print(prompt);
        }

        private void insert() throws IOException {
            if (args.length != 4) {
                System.out.println("Error : insert requires two arguments [key] and [value].");
                return;
            } else {

                args[3] += "," + id;

                int key;
                String value = args[2];
                try {
                    key = Integer.parseInt(args[1]);
                } catch (NumberFormatException e) {
                    System.out.println("Error : " + args[1] + " is not a number.");
                    return;
                }

                if (isInRange(key)) {
                    boolean isAvailable = false;
                    String curValue;
                    synchronized (mapMutex) {
                        curValue = map[key];
                        if (curValue == null) {
                            map[key] = value;
                            isAvailable = true;
                        }
                    }

                    String message = PRINT + " " + parseTrace(args[3], true);
                    if (isAvailable) {
                        message += "Successfully added (" + key + ", " + value + ") to Name Server " + id;
                    } else {
                        message += "Error : Value \"" + curValue + "\" already associated with key " + key;
                    }
                    sendMessage(message, bootIp, bootPort);
                } else {
                    String message = INSERT + " " + key + " " + value + " " + args[3];
                    sendMessage(message, getNextIp(), getNextPort());
                }
            }
        }

        private void delete() throws IOException {

            if (args.length != 3) {
                System.out.println("Error : Expected a key to delete from the System, and a trace string.");
                return;
            }

            // Update trace!
            args[2] += "," + id;

            int key;
            try {
                key = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.out.println("Error : " + args[1] + " is not a number.");
                return;
            }

            if (isInRange(key)) {
                synchronized (mapMutex) {
                    String value = map[key];
                    if (value == null) {

                        String trace = parseTrace(args[2], true);
                        String message = PRINT + " " + trace + "Key not found.";
                        sendMessage(message, bootIp, bootPort);
                    } else {
                        map[key] = null;
                        String trace = parseTrace(args[2], true);
                        String message = PRINT + " " + trace + "Successful deletion";
                        sendMessage(message, bootIp, bootPort);
                    }
                }
            } else {
                // System.out.println("Calling DELETE on next name server");

                // COMMAND CONSTANT
                String message = DELETE + " " + key + " " + args[2];
                sendMessage(message, getNextIp(), getNextPort());
            }

        }
    }

    /* ******* GENERAL HELPER FUNCTIONS ******* */

    protected String parseTrace(String s, boolean checkFirst) {

        StringBuilder ret = new StringBuilder();
        // ret.append("Name Server Trace:\n");

        String[] ids = s.split(",");
        if (ids.length >= 1) {

            if (checkFirst) {
                ret.append("\tChecked Name Server ");
                ret.append(ids[0]);
                ret.append("\n");
            }

            for (int i = 1; i < ids.length; i++) {
                ret.append("\tContacted Name Server ");
                ret.append(ids[i]);
                ret.append("\n");
            }
        }

        return ret.toString();
    }

    protected boolean isInRange(int key) {
        return (id <= key && key <= endIndex);
    }


    /* ******* STUB MUTEX WRAPPER FUNCTIONS ******* */

    protected String getPrevIp() {
        String ip;
        synchronized (prevMutex) {
            if (prevServer == null)
                return "";
            ip = prevServer.getIp();
        }
        return ip;
    }

    protected int getPrevPort() {
        int port;
        synchronized (prevMutex) {
            if (prevServer == null)
                return -1;
            port = prevServer.getPort();
        }
        return port;
    }

    protected int getPrevId() {
        int id;
        synchronized (prevMutex) {
            if (prevServer == null)
                return -1;
            id = prevServer.getId();
        }
        return id;
    }

    protected String getNextIp() {
        String ip;
        synchronized (nextMutex) {
            if (nextServer == null)
                return "";
            ip = nextServer.getIp();
        }
        return ip;
    }

    protected int getNextPort() {
        int port;
        synchronized (nextMutex) {
            if (nextServer == null)
                return -1;
            port = nextServer.getPort();
        }
        return port;
    }

    protected int getNextId() {
        int id;
        synchronized (nextMutex) {
            if (nextServer == null)
                return -1;
            id = nextServer.getId();
        }
        return id;
    }

    protected class NameServerStub {

        private int id;
        private String ip;
        private int port;

        NameServerStub(String ip, int port, int id) {
            this.ip = ip;
            this.port = port;
            this.id = id;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

    }

    /* ******* SOCKET WRAPPER METHODS ******* */

    protected void sendMessage(String message, String ip, int port) throws IOException {
        Socket socket = connect(ip, port, 60);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        out.writeUTF(message);
        socket.close();
        out.close();
        try {
            Thread.sleep(25);
        } catch (InterruptedException e) {
            //
        }
    }

    protected Socket connect(String ip, int port, int maxTime) {
        Socket newSocket;
        int attempts = 0;
        while (true) {
            try {
                newSocket = new Socket(ip, port);
                break;
            } catch (IOException e) {
                // Errors expected, simply tries again 1 second later
            }
            if (attempts >= maxTime)
                return null;

            attempts++;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return newSocket;
    }
}








