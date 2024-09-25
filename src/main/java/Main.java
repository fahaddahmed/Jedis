import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
    public static void main(String[] args) {
        // Logs will appear here for debugging purposes
        System.out.println("Logs will appear here!");

        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        int port = 6379;

        try {
            // Initialize the server socket on port 6379
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);  // Avoid "Address already in use" errors

            // Wait for a connection from a client
            clientSocket = serverSocket.accept();

            // Setup BufferedReader to read input from the client
            BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

            // Setup OutputStream to send responses to the client
            OutputStream outputStream = clientSocket.getOutputStream();

            String userInput;
            // Continuously listen for commands from the client
            while ((userInput = reader.readLine()) != null) {
                String pong = "+PONG\r\n";

                // Respond with PONG if the client sends PING
                if (userInput.equals("PING")) {
                    outputStream.write(pong.getBytes());
                    outputStream.flush();  // Ensure the response is sent immediately
                }
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();  // Close the client connection
                }
            } catch (IOException e) {
                System.out.println("IOException during socket close: " + e.getMessage());
            }
        }
    }
}