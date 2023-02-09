package com.flatide.floodgate;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

public class CAEnv {
    String address = "";
    String addressLast = "";
    String port = "";

    private CAEnv() {
    }

    public static CAEnv getInstance() {
        return LazyHolder.instance;
    }

    private static class LazyHolder {
        private static final CAEnv instance = new CAEnv();
    }

    public String getAddress() {
        if( address.isEmpty()) {
            this.address = getLocalIp();
            this.addressLast = this.address.substring( this.address.lastIndexOf(".") + 1 );
        }
        
        return this.address;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getPort() {
        return port;
    }

    public String getAddressLat() {
        if( this.addressLast.isEmpty() ) {
            this.address = getLocalIp();
            this.addressLast = this.address.substring( this.address.lastIndexOf(".") + 1);
        }

        return String.format("%03d", Integer.parseInt(this.addressLast));
    }

    public String getAddressLastWithPort() {
        if( this.addressLast.isEmpty() ) {
            this.address = getLocalIp();
            this.addressLast = this.address.substring( this.address.lastIndexOf(".") + 1);
        }

        return String.format("%03d%05d",Integer.parseInt(this.addressLast), Integer.parseInt(getPort()) );
    }

    private String getLocalIp() {
        try {
            for( Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces(); en.hasMoreElements(); ) {
                NetworkInterface intf = en.nextElement();
                for(Enumeration<InetAddress> enumIpAddr = intf.getInetAddresses(); enumIpAddr.hasMoreElements(); ) {
                    InetAddress inetAddress = enumIpAddr.nextElement();
                    if( !inetAddress.isLoopbackAddress() && !inetAddress.isLinkLocalAddress() && inetAddress.isSiteLocalAddress()) {
                        return inetAddress.toString();
                    }
                }
            }
            return InetAddress.getLocalHost().getHostAddress();
        } catch(Exception e ) {
        }

        return "";
    }
}
