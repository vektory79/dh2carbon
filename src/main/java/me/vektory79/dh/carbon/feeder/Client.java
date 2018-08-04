package me.vektory79.dh.carbon.feeder;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Client implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(Client.class.getName());
    private final String address;
    private final int port;
    private Socket socket;
    private DataOutputStream socketOutput;
    private DataInputStream socketInput;
    private final StringBuilder buffer = new StringBuilder(10240);


    public Client(String address, int port) {
        this.address = address;
        this.port = port;
        connect();
    }

    public void collect(String metric, String value) {
        long now = System.currentTimeMillis() / 1000;
        buffer
                .append(metric)
                .append(' ')
                .append(value)
                .append(' ')
                .append(now)
                .append('\n');
    }

    public void submit() {
        if (socketOutput == null) {
            connect();
        }
        while (true) {
            try {
                socketOutput.writeUTF(buffer.toString());
                socketOutput.flush();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, e, () -> "Can't commit metrics");
                connect();
                sleep();
                continue;
            }
            break;
        }
        buffer.setLength(0);
    }

    @Override
    public void close() {
        close(socketInput);
        close(socketOutput);
        close(socket);
        socketInput = null;
        socketOutput = null;
        socket = null;
    }

    public boolean isClosed() {
        return socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown();
    }

    private void connect() {
        close();
        while (true) {
            try {
                socket = new Socket(address, port);
                socketOutput = new DataOutputStream(socket.getOutputStream());
                socketInput = new DataInputStream(socket.getInputStream());
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, e, () -> "Can't connect object");
                close();
                sleep();
                continue;
            }
            break;
        }
    }

    private void close(AutoCloseable object) {
        try {
            if (object != null) {
                object.close();
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, e, () -> "Can't close object");
        }
    }

    private void sleep() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            LOGGER.log(Level.WARNING, e, () -> "Can't wait");
        }
    }
}
