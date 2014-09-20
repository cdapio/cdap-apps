/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package co.cask.cdap.netlens.app;

import java.net.InetAddress;

/**
 *
 */
public final class PacketData {

  private final String id;
  private final long timestamp;
  private final Type type;
  private final Address sourceAddress;
  private final Address destAddress;
  private final String timeZone;

  public enum Type {
    TCP,
    UDP;
  }

  public final class Address {
    private final InetAddress ip;
    private final String hostname;
    private final int port;

    public Address(InetAddress ip, String hostname, int port) {
      this.ip = ip;
      this.hostname = hostname;
      this.port = port;
    }

    public InetAddress getIP() {
      return ip;
    }

    public String getHostname() {
      return hostname;
    }

    public int getPort() {
      return port;
    }
  }

  public PacketData(String id, long timestamp, Type type, Address sourceAddress, Address destAddress, String timeZone) {
    this.id = id;
    this.timestamp = timestamp;
    this.type = type;
    this.sourceAddress = sourceAddress;
    this.destAddress = destAddress;
    this.timeZone = timeZone;
  }

  public String getId() {
    return id;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public Type getType() {
    return type;
  }

  public Address getSourceAddress() {
    return sourceAddress;
  }

  public Address getDestAddress() {
    return destAddress;
  }

  public String getTimeZone() {
    return timeZone;
  }
}
