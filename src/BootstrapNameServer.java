import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Scanner;

public class BootstrapNameServer extends NameServer {

    /* ******* MAIN ******* */

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Error : Bootstrap Name Server requires config file argument");
            System.exit(0);
        }
        BootstrapNameServer bootstrap = new BootstrapNameServer(args[0]);
    }

    /* ******* CONSTRUCTOR ******* */

    public BootstrapNameServer(String configFile) {

        parseConfigFile(configFile);

        // Init Bootstrap as FIRST and ONLY Name Server
        endIndex = id - 1;
        if (endIndex == -1)
            endIndex = HASH_SIZE -1;

        nextServer = new NameServerStub(ip, listenPort, 0);
        prevServer = new NameServerStub(ip, listenPort, 0);

        prompt = "Bootstrap > ";

        System.out.println("ID: " + id);
        System.out.println("Port: " + listenPort);

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

        bootIp = ip;

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

            bootPort = listenPort;

            config.nextLine();
            while (config.hasNext()) {
                String[] pairs = config.nextLine().split(" ");
                int key = Integer.parseInt(pairs[0]);
                String value = pairs[1];
                synchronized (mapMutex) {
                    map[key] = value;
                }
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.out.println("Error : File not found");
            System.exit(0);
        }

    }

    private class UserThread extends Thread implements Runnable {

        UserThread() {

        }

        @Override
        public void run() {

            Scanner in = new Scanner(System.in);
            String input;

            // Until break...
            boolean running = true;
            while (running) {

                System.out.print(prompt);

                input = in.nextLine();

                String[] tokens = input.split(" ");

                if (tokens.length >= 1) {

                    String command = tokens[0].toLowerCase();

                    try {
                        // out.writeUTF(command);
                        switch (command) {
                            case "lookup":
                                if (tokens.length != 2)
                                    System.out.println("Error : lookup requires a single argument [key].");
                                else
                                    try {
                                        lookup(Integer.parseInt(tokens[1]));
                                    } catch (NumberFormatException e) {
                                        System.out.println("Error : " + tokens[1] + " is not a number.");
                                    }
                                break;
                            case "insert":
                                if (tokens.length != 3) {
                                    System.out.println("Error : insert requires two arguments [key] and [value].");
                                } else {
                                    try {
                                        insert(Integer.parseInt(tokens[1]), tokens[2]);
                                    } catch (NumberFormatException e) {
                                        System.out.println("Error : " + tokens[1] + " is not a number.");
                                    }
                                }
                                break;
                            case "delete":
                                if (tokens.length != 2) {
                                    System.out.println("Error : delete requires a single argument [key].");
                                } else {
                                    try {
                                        delete(Integer.parseInt(tokens[1]));
                                    } catch (NumberFormatException e) {
                                        System.out.println("Error : " + tokens[1] + " is not a number.");
                                    }
                                }
                                break;
                            case "status":
                                printMap();
                                System.out.println("\nCovering the range " + id + " to " + endIndex);
                                System.out.println("Previous Name Server: " + prevServer.getId());
                                System.out.println("Next Name Server: " + nextServer.getId());
                                break;
                            case "":
                                break;
                            case "quit":
                            case "exit":
                                quit();
                                running = false;
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
            System.out.println("\nGoodbye.");
        }

        private void lookup(int key) throws IOException {

            if (key < 0 || key >= HASH_SIZE) {
                System.out.println("Error : Invalid key");
                return;
            }

            if (isInRange(key)) {
                String value;
                synchronized (mapMutex) {
                    value = map[key];
                }
                if (value == null) {
                    System.out.print(parseTrace(Integer.toString(id), true));
                    System.out.println("Key not found.");
                } else {
                    System.out.print(parseTrace(Integer.toString(id), true));
                    System.out.println("Success! Found \"" + value + "\" on Name Server " + id);
                }
            } else {
                // System.out.println("Calling LOOKUP on next name server");
                String trace = Integer.toString(id);
                String message = LOOKUP + " " + key + " " + trace;
                sendMessage(message, getNextIp(), getNextPort());
            }
        }

        private void insert(int key, String value) throws IOException {

            if (key < 0 || key >= HASH_SIZE) {
                System.out.println("Error : Invalid key");
                return;
            }

            if (isInRange(key)) {
                synchronized (mapMutex) {
                    if (map[key] == null) {
                        map[key] = value;
                        System.out.print(parseTrace(Integer.toString(id), true));
                        System.out.println("Successfully added (" + key + ", " + value + ") to Name Server " + id);
                    } else {
                        System.out.print(parseTrace(Integer.toString(id), true));
                        System.out.println("Error : Value \"" + map[key] + "\" already associated with key " + key);
                    }
                }
            } else {
                // System.out.println("Calling INSERT on next name server");

                // COMMAND CONSTANT
                String trace = Integer.toString(id);
                String message = INSERT + " " + key + " " + value + " " + trace;
                sendMessage(message, getNextIp(), getNextPort());
            }
        }

        private void delete(int key) throws IOException {

            if (key < 0 || key >= HASH_SIZE) {
                System.out.println("Error : Invalid key");
                return;
            }

            if (isInRange(key)) {
                synchronized (mapMutex) {
                    String value = map[key];
                    if (value == null) {
                        System.out.print(parseTrace(Integer.toString(id), true));
                        System.out.println("Key not found.");
                    } else {
                        map[key] = null;
                        System.out.print(parseTrace(Integer.toString(id), true));
                        System.out.println("Successful deletion");
                    }
                }


            } else {
                // System.out.println("Calling DELETE on next name server");

                // COMMAND CONSTANT
                String trace = Integer.toString(id);
                String message = DELETE + " " + key + " " + trace;
                sendMessage(message, getNextIp(), getNextPort());
            }
        }

        private void quit() throws IOException {
            // Signal to nextServer to quit
            // Resume on reply and wait for response from last quitting server
        }
    }
}
