import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;


public class Main {

    private static final int PORT = 6379;
    private static final String PONG_RESPONSE = "+PONG\r\n";
    private static final HashMap<String, ValueWithExpiry> storeSet = new HashMap<>(); // Store with value and expiry time
    private static final String NULL_RESPONSE = "$-1\r\n";

    public static void main(String[] args) {
        try {
            // Open a non-blocking server socket channel
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false); // Non-blocking mode
            serverSocketChannel.socket().bind(new InetSocketAddress(PORT));
            
            // Open a selector to manage multiple channels
            Selector selector = Selector.open();
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT); // Register to accept connections
            
            System.out.println("Server is listening on port " + PORT);

            // Main event loop
            while (true) {
                // Wait for events (with timeout)
                selector.select();  // This blocks until at least one channel is ready

                // Get the set of ready-to-process events
                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

                while (keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();
                    keyIterator.remove();  // Remove the key to avoid processing it again

                    if (key.isAcceptable()) {
                        // Accept new client connection
                        handleAccept(serverSocketChannel, selector);
                    }

                    if (key.isReadable()) {
                        // Read client input and respond
                        handleRead(key);
                    }
                }
            }

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    private static void handleAccept(ServerSocketChannel serverSocketChannel, Selector selector) throws IOException {
        // Accept the client connection
        SocketChannel clientChannel = serverSocketChannel.accept();
        clientChannel.configureBlocking(false);  // Set the new client connection to non-blocking mode
        
        // Register the new client connection with the selector for reading
        clientChannel.register(selector, SelectionKey.OP_READ);
        System.out.println("New client connected: " + clientChannel.getRemoteAddress());
    }

    public static List<String> parseRESP(String input) {
        input = input.replace("\r\n", ",");
        String[] parts = input.split(",");
        List<String> parsedElements = new ArrayList<>();

        int index = 0;
        
        // Check if it starts with '*', indicating the number of elements
        if (parts[index].startsWith("*")) {
            int numElements = Integer.parseInt(parts[index].substring(1));
            index++;

            // Loop through and extract the elements
            while (parsedElements.size() < numElements) {
                if (parts[index].startsWith("$")) {
                    // Skip the length part as it's for validation, then add the actual data
                    index++; // Move to the actual data after length indicator
                    parsedElements.add(parts[index]); // Add the command or argument
                    index++; // Move to the next part
                }
            }
        }
        return parsedElements;
    }

    private static void handleRead(SelectionKey key) throws IOException {
        SocketChannel clientChannel = (SocketChannel) key.channel();

        ByteBuffer buffer = ByteBuffer.allocate(256);
        int bytesRead = clientChannel.read(buffer);  // Read client data into buffer

        if (bytesRead == -1) {
            // Client has closed the connection, so close the channel
            System.out.println("Client disconnected: " + clientChannel.getRemoteAddress());
            clientChannel.close();
            key.cancel();
            return;
        }

        // Prepare buffer for reading
        buffer.flip();  
        String request = new String(buffer.array(), 0, buffer.limit()).trim();  // Convert buffer to a string
        System.out.println("Received command: " + request);

        // Parse the RESP command using the parseRESP method
        List<String> parsedElements = parseRESP(request);
        if (parsedElements.isEmpty()) {
            return;
        }
        
        String command = parsedElements.get(0).toLowerCase();  // Get command name and make it case insensitive
        
        if (command.equals("echo")) {
            if (parsedElements.size() <= 1) {
                return;
            }

            String echoArgument = parsedElements.get(1);
            String echoResponse = "$" + echoArgument.length() + "\r\n" + echoArgument + "\r\n";
            
            ByteBuffer echoBuffer = ByteBuffer.wrap(echoResponse.getBytes());
            clientChannel.write(echoBuffer);  // Respond to client
        } else if (command.equals("ping")) {
            // Respond with PONG
            ByteBuffer responseBuffer = ByteBuffer.wrap(PONG_RESPONSE.getBytes());
            clientChannel.write(responseBuffer);
        } else if (command.equals("set")) {
            handleSetCommand(parsedElements, clientChannel);
        } else if (command.equals("get")) {
            String userGivenKey = parsedElements.get(1); 

            String valueFromStore = getValueWithExpiryCheck(userGivenKey);

            if (valueFromStore == null) {
                ByteBuffer responseBuffer = ByteBuffer.wrap(NULL_RESPONSE.getBytes());
                clientChannel.write(responseBuffer);
                return;
            }
            
            ByteBuffer encodedValue = encodeStringAsRESP(valueFromStore);
            clientChannel.write(encodedValue);
        }
        
    }

    /**
     * Handles the SET command, including optional PX expiry argument.
     * 
     * @param parsedElements The parsed elements of the SET command.
     * @param clientChannel The client channel to send a response to.
     */
    private static void handleSetCommand(List<String> parsedElements, SocketChannel clientChannel) throws IOException {
        String key = parsedElements.get(1);
        String value = parsedElements.get(2);
        long expiryTime = -1; // Default expiry time (-1 means no expiry)

        // Check if there's an expiry option
        if (parsedElements.size() >= 4 && parsedElements.get(3).equalsIgnoreCase("px")) {
            expiryTime = System.currentTimeMillis() + Long.parseLong(parsedElements.get(4));
        }

        // Store the value with the expiry time
        storeSet.put(key, new ValueWithExpiry(value, expiryTime));

        // Send the response to the client
        String setResponse = "+OK\r\n";
        ByteBuffer setResponseBuffer = ByteBuffer.wrap(setResponse.getBytes());
        clientChannel.write(setResponseBuffer);
    }

    /**
     * Retrieves the value for a given key, checking if it has expired.
     * 
     * @param key The key to retrieve the value for.
     * @return The value or null if the key does not exist or has expired.
     */
    private static String getValueWithExpiryCheck(String key) {
        ValueWithExpiry valueWithExpiry = storeSet.get(key);

        if (valueWithExpiry == null) {
            return null;  // Key does not exist
        }

        // Check if the key has expired
        if (valueWithExpiry.expiryTime != -1 && System.currentTimeMillis() > valueWithExpiry.expiryTime) {
            storeSet.remove(key);  // Remove the expired key
            return null;  // Key has expired
        }

        return valueWithExpiry.value;
    }

    /**
     * Encodes a string as a RESP string or a null bulk string.
     * @param input String to encode
     * @return RESP encoded string or null bulk string if input is null.
     */
    private static ByteBuffer encodeStringAsRESP(String input) {
        if (input == null) {
            return ByteBuffer.wrap("$-1\r\n".getBytes());  // Null bulk string
        }
        String encodedString = "$" + input.length() + "\r\n" + input + "\r\n";
        return ByteBuffer.wrap(encodedString.getBytes());
    }
}
