package com.didi.etc;

/**
 * @author 宿荣全.滴滴智能交通云
 * @data 2016年12月12日 下午3:43:42  
 * Description:<p>  <／p>
 * @version 1.0
 * @since JDK 1.7.0_80
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

//import sun.misc.*;
import sun.misc.BASE64Encoder;
import sun.misc.BASE64Decoder;

/**
 * AESTest.java
 * 
 * @author Techzero
 * @Email techzero@163.com
 * @Time 2013-12-12 下午1:25:44
 */
@SuppressWarnings("unused")
public class test {
	private static BASE64Decoder base64Decoder = new BASE64Decoder();
	public static void main(String[] args) throws IOException {
		
//		txt2StringForJIE("/Users/didi/work/test/pwd_20161125113411.data");
		MD5 md5 = new MD5();
		System.out.println(md5.getMD5("566062027384311"));
	}
	
	
	 /**
     * 解密：
     * @param fileName
     * @throws IOException
     */
	public static void txt2StringForJIE(String fileName) throws IOException {
		File file = new File(fileName); //需要解密的文件路径和名字
		FileWriter fw = null;
		BufferedReader br = null;
		try {
			fw = new FileWriter("/Users/didi/work/test/"+"解密后.data");//解密后文件存放位置 和文件名  
			br = new BufferedReader(new FileReader(file));// 构造一个BufferedReader类来读取加密后的文件
			String s = null;
			String pwd = null;
			String data = null;
			String pos = "1,3,16,13,5,7";//位置信息（可以作为参数）
			String[] strs = null;
			while ((s = br.readLine()) != null) {// 使用readLine方法，一次读一行
				String key = "";
				// 解密
				System.out.println(s);
				pwd = s.substring(0,16);//密码区
				data = s.substring(16);//数据信息区
				strs = pos.split(",");
				for(int i=0;i<strs.length;i++){
					key+=String.valueOf(pwd.charAt(Integer.parseInt(strs[i])-1));
				}
				System.out.println(key);
			    byte[] encrypted = base64Decoder.decodeBuffer(data);
				byte[] resu = decrypt(encrypted, key);
				String result = new String(resu,"gbk");
				fw.write(result+"\r\n");
			}
			br.close();
			fw.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
				fw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	
	/**
	 * 解密AES加密过的字符串
	 * 
	 * @param content
	 *            AES加密过过的内容
	 * @param password
	 *            加密时的密码
	 * @return 明文
	 * @throws UnsupportedEncodingException 
	 */
	public static byte[] decrypt(byte[] content, String password) throws UnsupportedEncodingException {
	
		try {
			KeyGenerator kgen = KeyGenerator.getInstance("AES");// 创建AES的Key生产者
/*			kgen.init(128, new SecureRandom(password.getBytes()));
*/			SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
	        random.setSeed(password.getBytes());
			kgen.init(128, random);
			SecretKey secretKey = kgen.generateKey();// 根据用户密码，生成一个密钥
			byte[] enCodeFormat = secretKey.getEncoded();// 返回基本编码格式的密钥
			SecretKeySpec key = new SecretKeySpec(enCodeFormat, "AES");// 转换为AES专用密钥
			Cipher cipher = Cipher.getInstance("AES");// 创建密码器
			cipher.init(Cipher.DECRYPT_MODE, key);// 初始化为解密模式的密码器
			byte[] result = cipher.doFinal(content);
			return result; // 明文

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (NoSuchPaddingException e) {
			e.printStackTrace();
		} catch (InvalidKeyException e) {
			e.printStackTrace();
		} catch (IllegalBlockSizeException e) {
			e.printStackTrace();
		}
         catch (BadPaddingException e) {
			e.printStackTrace();
		}
		return null;
	}

	
	
	
//	private static BASE64Decoder base64Decoder = new BASE64Decoder();
//
//	public static void main(String[] args) throws IOException {
//
////		String fileName = "20161125113411-yexuRvj2B6z7TB35QXnTvQ==-krwYE_3wf0NFs96z8Z0+JA==-yexuRvj2B6z7TB35QXnTvQ==-krwYE_3wf0NFs96z8Z0+JA==-yexuRvj2B6z7TB35QXnTvQ==-.data";
////
////		String code = fileName.replaceAll("-", "/");
////		byte[] codes = base64Decoder.decodeBuffer("yexuRvj2B6z7TB35QXnTvQ==");
////		code = new String(codes, "gbk");
//		// byte[] codeByte = decrypt(codes, "mima");
//		// code = new String(codeByte, "gbk");
////		System.out.println(code);
//		
//		
//		txt2StringForJIE("/Users/didi/work/20161125113411-yexuRvj2B6z7TB35QXnTvQ==-krwYE_3wf0NFs96z8Z0+JA==-yexuRvj2B6z7TB35QXnTvQ==-krwYE_3wf0NFs96z8Z0+JA==-yexuRvj2B6z7TB35QXnTvQ==-.data");
//		
//	}
//
//	// 解密
//	public static String getFromBase64(String s) {
//		byte[] b = null;
//		String result = null;
//		if (s != null) {
//			BASE64Decoder decoder = new BASE64Decoder();
//			try {
//				b = decoder.decodeBuffer(s);
//				result = new String(b, "utf-8");
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//		return result;
//	}
//
//	public static byte[] decrypt(byte[] content, String password)
//			throws UnsupportedEncodingException {
//		try {
//			KeyGenerator kgen = KeyGenerator.getInstance("AES");// 创建 AES 的 Key
//																// 生产者
//
//			// kgen.init(128, new SecureRandom(password.getBytes()));
//			SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
//			random.setSeed(password.getBytes());
//			kgen.init(128, random);
//			SecretKey secretKey = kgen.generateKey();// 根据用户密码,生成一个密钥
//			byte[] enCodeFormat = secretKey.getEncoded();// 返回基本编码格式的密钥
//			SecretKeySpec key = new SecretKeySpec(enCodeFormat, "AES");// 转换为
//																		// AES专用密钥
//			Cipher cipher = Cipher.getInstance("AES");// 创建密码器
//			cipher.init(Cipher.DECRYPT_MODE, key);// 初始化为解密模式的密码器
//			byte[] result = cipher.doFinal(content);
//			return result; // 明文
//
//		} catch (NoSuchAlgorithmException e) {
//			e.printStackTrace();
//		} catch (NoSuchPaddingException e) {
//			e.printStackTrace();
//		} catch (InvalidKeyException e) {
//			e.printStackTrace();
//		} catch (IllegalBlockSizeException e) {
//			e.printStackTrace();
//		} catch (BadPaddingException e) {
//			e.printStackTrace();
//		}
//		return null;
//	}
//
//	/**
//	 * 解密原理:
//	 * <br>文件采用 AES 128 位加密算法进行加密后用 BASE64 进行编码 , 解密密码 经过加密处理存放在文件名中 。</br>
//	 * <br>用户取得文件后只需,解密文件名中的密文信息,得到文件解密密码 即可对文件进行解密。 文件名以短横杠"-"分割为几部分,</br>
//	 * <br>格式为:时间序列-密码密文 1-密码密文 2-密码密文 3-密码密文n-.data。</br>
//	 * <br>用户取得文件名后,根据自己的位置信息得到加密密码密文。 之后用自己的解密密码 进行解密得到文件密码。</br>
//	*/
//	public static void txt2StringForJIE(String fileName) throws IOException {
//		File file = new File(fileName); // 需要解密的文件路径和名字
//		String code = "";
//		// 从文件名中取到自己的 24 位的密文信息 该例子中用户的位置信息是 1 ,则 取的是第一个 "-" 和第二个"-"之间的密文信息;
//		// 用户位置信息
//		int pos = 1; 
//		int a = fileName.indexOf("-");
//		int b = 0;
//		int c = 0;
//		for (int i = 0; i < pos; i++) {
//			System.out.println(i);
//			c = a;
//			a = fileName.indexOf("-", a + 1);
//			if (i == pos - 1) {
//				b = a;
//			}
//		}
//		 // 用户自己的解密密码
//		code = fileName.replaceAll("_", "/").substring(c + 1, b);
//		String mima = "9588028820109132570743325311898426347857298773549468758875018579537757772163084478873699447306034466200616411960574122434059469100235892702736860872901247123456";
//		byte[] codes = base64Decoder.decodeBuffer(code);
//		byte[] codeByte = decrypt(codes, mima);
//		// 解密密文后得到的文件密码 
//		code = new String(codeByte, "gbk"); 
//		//若该密码全为0则说明用户无权限读取该文件
//		FileWriter fw = null;
//		BufferedReader br = null;
//		try {
//			// 解密后文件存放位置 和文件名
//			fw = new FileWriter(fileName + "-jiemi");
//			// 构造一个 BufferedReader
//			br = new BufferedReader(new FileReader(file));
//			// 类来读取加密后的文件
//			String s = null;
//			// 使用 readLine 方法,一次读一行
//			while ((s = br.readLine()) != null) {
//				// 解密
//				byte[] encrypted = base64Decoder.decodeBuffer(s);
//				byte[] resu = decrypt(encrypted, code);
//				String result = new String(resu, "gbk");
//				fw.write(result + "\r\n");
//			}
//			br.close();
//			fw.close();
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally {
//			try {
//				br.close();
//				fw.close();
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
//	}

}