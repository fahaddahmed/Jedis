import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class Main {

    private static final int PORT = 6379;
    private static final String PONG_RESPONSE = "+PONG\r\n";
    private static final HashMap<String, ValueWithExpiry> storeSet = new HashMap<>(); // Store with value and expiry
    private static final String NULL_RESPONSE = "$-1\r\n";
    private static String dir;
    private static String dbfilename;

    public static void main(String[] args) {
        setDatabaseConfig(args);
        try {
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.socket().bind(new InetSocketAddress(PORT));

            Selector selector = Selector.open();
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

            System.out.println("Server is listening on port " + PORT);

            while (true) {
                selector.select();
                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

                while (keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();
                    keyIterator.remove();

                    if (key.isAcceptable()) {
                        handleAccept(serverSocketChannel, selector);
                    }

                    if (key.isReadable()) {
                        handleRead(key);
                    }
                }
            }

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    private static void setDatabaseConfig(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--dir") && i + 1 < args.length) {
                dir = args[i + 1];
            }
            if (args[i].equals("--dbfilename") && i + 1 < args.length) {
                dbfilename = args[i + 1];
            }
        }
    
        // Provide default values if the arguments are missing
        if (dir == null) {
            dir = ".";  // Default to current directory
        }
        if (dbfilename == null) {
            dbfilename = "dump.rdb";  // Default to "dump.rdb"
        }
    }
    

    private static void handleAccept(ServerSocketChannel serverSocketChannel, Selector selector) throws IOException {
        SocketChannel clientChannel = serverSocketChannel.accept();
        clientChannel.configureBlocking(false);
        clientChannel.register(selector, SelectionKey.OP_READ);
        System.out.println("New client connected: " + clientChannel.getRemoteAddress());
    }

    public static List<String> parseRESP(String input) {
        input = input.replace("\r\n", ",");
        String[] parts = input.split(",");
        List<String> parsedElements = new ArrayList<>();

        int index = 0;
        if (parts[index].startsWith("*")) {
            int numElements = Integer.parseInt(parts[index].substring(1));
            index++;

            while (parsedElements.size() < numElements && index < parts.length) {
                if (parts[index].startsWith("$")) {
                    index++;
                    if (index < parts.length) {
                        parsedElements.add(parts[index]);
                        index++;
                    }
                }
            }
        }
        return parsedElements;
    }

    private static void handleRead(SelectionKey key) throws IOException {
        SocketChannel clientChannel = (SocketChannel) key.channel();

        ByteBuffer buffer = ByteBuffer.allocate(256);
        int bytesRead = clientChannel.read(buffer);

        if (bytesRead == -1) {
            System.out.println("Client disconnected: " + clientChannel.getRemoteAddress());
            clientChannel.close();
            key.cancel();
            return;
        }

        buffer.flip();
        String request = new String(buffer.array(), 0, buffer.limit()).trim();
        System.out.println("Received command: " + request);

        List<String> parsedElements = parseRESP(request);
        if (parsedElements.isEmpty()) {
            return;
        }

        String command = parsedElements.get(0).toLowerCase();

        switch (command) {
            case "echo":
                if (parsedElements.size() > 1) {
                    String echoArgument = parsedElements.get(1);
                    String echoResponse = "$" + echoArgument.length() + "\r\n" + echoArgument + "\r\n";
                    clientChannel.write(ByteBuffer.wrap(echoResponse.getBytes()));
                }
                break;
            case "ping":
                clientChannel.write(ByteBuffer.wrap(PONG_RESPONSE.getBytes()));
                break;
            case "set":
                handleSetCommand(parsedElements, clientChannel);
                break;
            case "get":
                String userGivenKey = parsedElements.get(1);
                String valueFromStore = getValueWithExpiryCheck(userGivenKey);
                if (valueFromStore == null) {
                    valueFromStore = readValueFromRdb(userGivenKey);
                }
                clientChannel.write(encodeStringAsRESP(valueFromStore));
                break;
            case "keys":
                handleKeysCommand(clientChannel);
                break;
            case "config":
                if (parsedElements.size() > 1 && parsedElements.get(1).equalsIgnoreCase("get")) {
                    handleConfigGetCommand(parsedElements.get(2).toLowerCase(), clientChannel);
                }
                break;
        }
    }

    private static String readValueFromRdb(String key) {
        if (dir == null || dbfilename == null) {
            System.err.println("Error: Database directory or filename is not set.");
            return null;
        }
    
        File dbFile = new File(dir, dbfilename);
        if (!dbFile.exists()) {
            System.err.println("Error: RDB file not found: " + dbFile.getAbsolutePath());
            return null;
        }
    
        try (InputStream fis = new FileInputStream(dbFile)) {
            byte[] redis = new byte[5];
            byte[] version = new byte[4];
            fis.read(redis);
            fis.read(version);
            int b;
            while ((b = fis.read()) != -1) {
                if (b == 0xFF) {
                    break;
                } else if (b == 0xFE) {
                    fis.read();
                } else if (b == 0xFB) {
                    fis.readNBytes(lengthEncoding(fis, fis.read()));
                    fis.readNBytes(lengthEncoding(fis, fis.read()));
                    break;
                }
            }
            while ((b = fis.read()) != -1) {
                int strLength = lengthEncoding(fis, b);
                byte[] bytes = fis.readNBytes(strLength);
                String readKey = new String(bytes);
                if (key.equals(readKey)) {
                    int valueLength = lengthEncoding(fis, fis.read());
                    byte[] valueBytes = fis.readNBytes(valueLength);
                    return new String(valueBytes);
                }
                fis.readNBytes(lengthEncoding(fis, fis.read()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static void handleKeysCommand(SocketChannel clientChannel) throws IOException {
        String key = "foo";
        try (InputStream fis = new FileInputStream(new File(dir, dbfilename))) {
            byte[] redis = new byte[5];
            byte[] version = new byte[4];
            fis.read(redis);
            fis.read(version);
            System.out.println("Magic String = " + new String(redis, StandardCharsets.UTF_8));
            System.out.println("Version = " + new String(version, StandardCharsets.UTF_8));
            int b;
            while ((b = fis.read()) != -1) {
                if (b == 0xFF) {
                    System.out.println("EOF");
                    break;
                } else if (b == 0xFE) {
                    System.out.println("SELECTDB");
                } else if (b == 0xFB) {
                    System.out.println("RESIZEDB");
                    b = fis.read();
                    fis.readNBytes(lengthEncoding(fis, b));
                    fis.readNBytes(lengthEncoding(fis, b));
                    break;
                }
            }
            System.out.println("header done");
            while ((b = fis.read()) != -1) {
                int strLength = lengthEncoding(fis, b);
                byte[] bytes = fis.readNBytes(strLength);
                key = new String(bytes);
                break;
            }
        }
        clientChannel.write(ByteBuffer.wrap(String.format("*1\r\n$%d\r\n%s\r\n", key.length(), key).getBytes()));
    }

    private static int lengthEncoding(InputStream is, int b) throws IOException {
        int length;
        int first2bits = b & 0b11000000;
        if (first2bits == 0) {
            length = b & 0b00111111;
        } else if (first2bits == 0b01000000) {
            length = ((b & 0b00111111) << 8) | (is.read() & 0xFF);
        } else if (first2bits == 0b10000000) {
            ByteBuffer buffer = ByteBuffer.allocate(4);
            buffer.put(is.readNBytes(4));
            buffer.rewind();
            length = buffer.getInt();
        } else {
            length = -1; // Special format
        }
        return length;
    }

    private static void handleSetCommand(List<String> parsedElements, SocketChannel clientChannel) throws IOException {
        String key = parsedElements.get(1);
        String value = parsedElements.get(2);
        long expiryTime = -1;

        if (parsedElements.size() >= 4 && parsedElements.get(3).equalsIgnoreCase("px")) {
            expiryTime = System.currentTimeMillis() + Long.parseLong(parsedElements.get(4));
        }

        storeSet.put(key, new ValueWithExpiry(value, expiryTime));
        clientChannel.write(ByteBuffer.wrap("+OK\r\n".getBytes()));
    }

    private static void handleConfigGetCommand(String configKey, SocketChannel clientChannel) throws IOException {
        List<String> configList = new ArrayList<>();
        if (configKey.equals("dir")) {
            configList.add("dir");
            configList.add(dir);
        } else if (configKey.equals("dbfilename")) {
            configList.add("dbfilename");
            configList.add(dbfilename);
        }
        clientChannel.write(encodeStringAsRESPArray(configList));
    }

    private static String getValueWithExpiryCheck(String key) {
        ValueWithExpiry valueWithExpiry = storeSet.get(key);
        if (valueWithExpiry == null) {
            return null;
        }
        if (valueWithExpiry.expiryTime != -1 && System.currentTimeMillis() > valueWithExpiry.expiryTime) {
            storeSet.remove(key);
            return null;
        }
        return valueWithExpiry.value;
    }

    private static ByteBuffer encodeStringAsRESP(String input) {
        if (input == null) {
            return ByteBuffer.wrap(NULL_RESPONSE.getBytes());
        }
        return ByteBuffer.wrap(String.format("$%d\r\n%s\r\n", input.length(), input).getBytes());
    }

    private static ByteBuffer encodeStringAsRESPArray(List<String> input) {
        StringBuilder encodedString = new StringBuilder("*" + input.size() + "\r\n");
        for (String element : input) {
            encodedString.append("$").append(element.length()).append("\r\n").append(element).append("\r\n");
        }
        return ByteBuffer.wrap(encodedString.toString().getBytes());
    }
}
