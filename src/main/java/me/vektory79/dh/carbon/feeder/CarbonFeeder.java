package me.vektory79.dh.carbon.feeder;

import org.json.JSONArray;
import org.json.JSONObject;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.logging.Logger;

public class CarbonFeeder implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(CarbonFeeder.class.getName());

    private final List<Client> clients = new ArrayList<>();
    private final String address;
    private final int port;
    private ThreadLocal<Client> localClient = ThreadLocal.withInitial(this::createClient);

    public CarbonFeeder(String address, int port) {
        this.address = address;
        this.port = port;
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public void sendJson(String prefix, JSONObject json) {
        privateSendJson(prefix, json);
        getLocalClient().submit();
    }

    private void privateSendJson(String prefix, JSONObject json) {
        for (String key : json.keySet()) {
            Object obj = json.opt(key);
            if (obj == null) {
                continue;
            }
            String subPrefix = prefix + "." + key;
            sendValue(subPrefix, obj);
        }
    }

    public void sendValue(String subPrefix, Object obj) {
        if (obj instanceof Integer
                || obj instanceof Double
                || obj instanceof Float
                || obj instanceof Short
                || obj instanceof Byte) {
            DecimalFormat df = new DecimalFormat("0", DecimalFormatSymbols.getInstance(Locale.ENGLISH));
            df.setMaximumFractionDigits(340); // 340 = DecimalFormat.DOUBLE_FRACTION_DIGITS
            sendFormattedValue(subPrefix, df.format(obj));
        } else if (obj instanceof JSONObject) {
            privateSendJson(subPrefix, (JSONObject) obj);
        } else if (obj instanceof JSONArray) {
            JSONArray array = (JSONArray) obj;
            for (int i = 0; i < array.length(); i++) {
                Object value = array.get(i);
                if (value == null) {
                    continue;
                }
                sendValue(subPrefix + "." + i, value);
            }
        }
    }

    private void sendFormattedValue(String prefix, String value) {
//        System.out.println(prefix + " " + value);
        getLocalClient().collect(prefix, value);
    }

    @Override
    public void close() {
        for (Client client : clients) {
            if (!client.isClosed()) {
                client.close();
            }
        }
    }

    private Client createClient() {
        Client client = new Client(getAddress(), getPort());
        clients.add(client);
        return client;
    }

    private Client getLocalClient() {
        if (localClient.get().isClosed()) {
            localClient.remove();
        }
        return localClient.get();
    }
}
