package com.didi.util.auto;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * @author 宿荣全.滴滴智能交通云
 * @data 2016年11月21日 上午11:25:23  
 * Description:<p>  <／p>
 * @version 1.0
 * @since JDK 1.7.0_80
 */
public class CreateDataFile {

	public static DateFormat format = new SimpleDateFormat("yyyyMMdd_HHmmss_SSS"); 
	public static DateFormat dataformat = new SimpleDateFormat("yyyy_MM_dd HH:mm:ss:SSS"); 
	// 生成文件的路径
	public static String path ="/Users/didi/work/components/logdata/createData/oput1";
	//生成文件周期
	public static long interval = 2000;
	
	public static void main(String[] args) {
		File dir  = new File(path);
		
	    if (dir.isDirectory()) {
	    	for (File file:dir.listFiles())
	    		file.delete();
	    }
	    
		createFile(path);
	}
	
	@SuppressWarnings("static-access")
	static void createFile(String path) {
		long indexId =0;
		while (true) {
			String fileName = format.format(System.currentTimeMillis()) + ".txt";
			String fullName= path+System.getProperty("file.separator")+fileName;
			indexId = createData(fullName,indexId);
			try {
				Thread.currentThread().sleep(interval);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	static long createData(String path,long indexId) {
		try {
			FileWriter fw = new FileWriter(path,false);
			for (int i=0;i<100;i++) {
				indexId ++;
				fw.write(indexId+",did,基础平台部,智能交通云,宿荣全,"+dataformat.format(System.currentTimeMillis()));
				fw.write("\n");
				fw.flush();
			}
			fw.close();
			System.out.println("新生成数据文件（100条数据）："+path);
		} catch (IOException e) {
			e.printStackTrace();
		}  
	return indexId;
	}
}
