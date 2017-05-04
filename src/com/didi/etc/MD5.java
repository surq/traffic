package com.didi.etc;

/**
 * @author 宿荣全.滴滴智能交通云
 * @data 2017年1月9日 下午7:29:33  
 * Description:<p>  <／p>
 * @version 1.0
 * @since JDK 1.7.0_80
 */
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

 class MD5 {
    private final MessageDigest md;

    public MD5() {
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public String getMD5(String val) {
        if (md == null) {
            return null;
        }
        try {
            // return hex (0-9,a-f); similar to convertToHex in MD5Hash
            return new BigInteger(1, md.digest(val.getBytes())).toString(16);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        MD5 md5 = new MD5();
        System.out.println(md5.getMD5("12145"));
    }

}