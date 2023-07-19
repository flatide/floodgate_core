package com.flatide.floodgate;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

public class FloodgateEnv {
    String address = "";
    String addressLast = "";
    String port = "";

    private FloodgateEnv() {
    }

    public static FloodgateEnv getInstance() {
        return LazyHolder.instance;
    }

    private static class LazyHolder {
        private static final FloodgateEnv instance = new FloodgateEnv();
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
        String ip = "";
        try {
            for( Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces(); en.hasMoreElements(); ) {
                NetworkInterface intf = en.nextElement();
                for(Enumeration<InetAddress> enumIpAddr = intf.getInetAddresses(); enumIpAddr.hasMoreElements(); ) {
                    InetAddress inetAddress = enumIpAddr.nextElement();
                    //System.out.println(inetAddress.toString());
                    if( !inetAddress.isLoopbackAddress() && !inetAddress.isLinkLocalAddress() && inetAddress.isSiteLocalAddress()) {
                        ip = inetAddress.toString();
                        //return inetAddress.toString();
                    }
                }
            }
            if (!ip.isEmpty()) {
                return ip;
            }
            return InetAddress.getLocalHost().getHostAddress();
        } catch(Exception e ) {
        }

        return "";
    }
}
