package com.flatide.floodgate.system;

public interface Security {
    public String encrypt(String msg);
    public String decrypt(String msg);
}
