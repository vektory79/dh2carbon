package me.vektory79.dh.carbon;

public class WatchDogException extends RuntimeException {
    public WatchDogException() {
        super("Device Hive message watch dog triggered");
    }
}
