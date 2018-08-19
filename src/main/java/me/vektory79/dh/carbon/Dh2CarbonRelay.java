package me.vektory79.dh.carbon;

import me.vektory79.dh.carbon.feeder.CarbonFeeder;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

@SuppressWarnings({"Duplicates", "squid:S106"})
public class Dh2CarbonRelay implements MqttCallback {
    private static final Logger LOGGER = Logger.getLogger(Dh2CarbonRelay.class.getName());

    private CarbonFeeder carbonFeeder;
    private MqttClient mqttClient;
    private final MqttConnectOptions connOpts = new MqttConnectOptions();

    private static final AtomicInteger watchCounter = new AtomicInteger(0);
    private String serverURI;
    private String clientId;
    private MqttClientPersistence persistence = new MemoryPersistence();

    public Dh2CarbonRelay(String serverURI, String clientId, String user, String password, String carbonServer, int carbonPort) {
        this.serverURI = serverURI;
        this.clientId = clientId;
        try {
            mqttClient = new MqttClient(serverURI, clientId, persistence);
        } catch (MqttException e) {
            throw new RuntimeException("Error creating MQTT client", e);
        }

        connOpts.setCleanSession(true);
        connOpts.setUserName(user);
        connOpts.setPassword(password.toCharArray());
        mqttClient.setCallback(this);

        carbonFeeder = new CarbonFeeder(carbonServer, carbonPort);
    }

    public static void main(String[] args) throws MqttException, InterruptedException, IOException {
        Properties props = new Properties();
        if (args.length == 1) {
            try (InputStream is = Files.newInputStream(Paths.get(args[0]), StandardOpenOption.READ)) {
                props.load(is);
            }
        } else {
            fallToHelp();
        }

        String serverURL = props.getProperty("server.url");
        String clientID = props.getProperty("client.id");
        String serverUser = props.getProperty("server.user");
        String serverPassword = props.getProperty("server.password");
        String carbonServer = props.getProperty("carbon.server");
        int carbonPort = Integer.parseInt(props.getProperty("carbon.port"));

        if (serverURL == null || clientID == null || serverUser == null || serverPassword == null) {
            fallToHelp();
        }

        Dh2CarbonRelay relay = new Dh2CarbonRelay(serverURL, clientID, serverUser, serverPassword, carbonServer, carbonPort);
        relay.connect();

        while (true) {
            Thread.sleep(1000);
            int counter = watchCounter.incrementAndGet();
            if (counter > 30) {
                watchCounter.set(0);
                relay.connectionLost(new WatchDogException());
            }
        }
    }

    private static void fallToHelp() {
        System.out.println("Incorrect parameter.");
        System.out.println();
        System.out.println("Execution example:");
        System.out.println("\tjava -jar dh2mqtt.jar <path/to/config.properties>");
        System.out.println();
        System.out.println("Required parameters in the file is:");
        System.out.println("\tserver.url - URL to the MQTT server. Ex: tcp://example.com:1883");
        System.out.println("\tserver.user - User name on the MQTT server");
        System.out.println("\tserver.password - User password on the MQTT server");
        System.out.println("\tclient.id - MQTT client ID");
        System.out.println("\tcarbon.server - The Carbon server name");
        System.out.println("\tcarbon.port - Port of the Carbon server");
        System.exit(-1);
    }

    private void connect() throws MqttException {
        mqttClient.connect(connOpts);
        subscribe();
    }

    private void subscribe() throws MqttException {
        mqttClient.subscribe("dh/#");
    }

    public void connectionLost(Throwable cause) {
        LOGGER.log(Level.SEVERE, "connection lost", cause);
        while (true) {
            try {
                Thread.sleep(5000);
                try {
                    mqttClient.close(true);
                } catch (Throwable e1) {
                    LOGGER.log(Level.WARNING, e1, () ->"Can't close connection");
                }
                mqttClient = new MqttClient(serverURI, clientId, persistence);
                connect();
            } catch (Throwable e) {
                LOGGER.log(Level.SEVERE, e, () -> "Can't restore connection");
                continue;
            }
            break;
        }
    }

    public void deliveryComplete(IMqttDeliveryToken token) {
        LOGGER.fine("delivery complete");
    }

    public void messageArrived(String topic, MqttMessage message) {
        if (!topic.equals("dh/request")) {
            return;
        }

        watchCounter.set(0);

        LOGGER.fine(() -> "message arrived [" + topic + "]: " + new String(message.getPayload()) + "'");

        StringBuilder mqttTopic = new StringBuilder(1024);
        mqttTopic.append("devices").append('/');

        JSONObject obj = new JSONObject(message.toString());
        if (obj.has("deviceId")) {
            String deviceId = obj.getString("deviceId");
            mqttTopic.append(deviceId).append('/');

            if (obj.has("notification")) {
                JSONObject notifObj = obj.getJSONObject("notification");
                mqttTopic.append("notification").append('/');

                String notification = notifObj.getString("notification");
                mqttTopic.append(notification);

                JSONObject params = notifObj.getJSONObject("parameters");
                carbonFeeder.sendJson(mqttTopic.toString().replace('/', '.'), params);
            }
        }
    }
}

