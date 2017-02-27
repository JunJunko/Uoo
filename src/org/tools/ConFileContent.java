package org.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;

public class ConFileContent {
	
	public static String readToString(String fileName) {  
        String encoding = "GBK";  
        File file = new File(fileName);  
        Long filelength = file.length();  
        byte[] filecontent = new byte[filelength.intValue()];  
        try {  
            FileInputStream in = new FileInputStream(file);  
            in.read(filecontent);  
            in.close();  
        } catch (FileNotFoundException e) {  
            e.printStackTrace();  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
        try {  
            return new String(filecontent, encoding);  
        } catch (UnsupportedEncodingException e) {  
            System.err.println("The OS does not support " + encoding);  
            e.printStackTrace();  
            return null;  
        }  
    } 
	
	
	public static void writeLog(String str)
    {
        try
        {
        String path="xml\\M_"+org.tools.GetProperties.getKeyValue("System")+"_"+org.tools.GetProperties.getKeyValue("TableNm")+".xml";
        File file=new File(path);
        if(!file.exists())
            file.createNewFile();
        FileOutputStream out=new FileOutputStream(file,false); //如果追加方式用true        
        StringBuffer sb=new StringBuffer();
        sb.append(str+"\n");
        out.write(sb.toString().getBytes("GBK"));//注意需要转换对应的字符集
        out.close();
        }
        catch(IOException ex)
        {
            System.out.println(ex.getStackTrace());
        }
    }    
	
	public static void ReplaceColumnNm(String filename){
		try {  
            FileInputStream in = new FileInputStream(filename);  
            InputStreamReader inReader = new InputStreamReader(in, "GBK");  
            BufferedReader bufReader = new BufferedReader(inReader);  
            String line = null;  
            int i = 1;  
            String reg=".*TARGETFIELD.*";  //判断字符串中是否含有ll
            while((line = bufReader.readLine()) != null){  
            	
            	if(line.matches(reg)){
                    System.out.println("第" + i + "行：" + line); 
            	}
                i++;  
            }  
            bufReader.close();  
            inReader.close();  
            in.close();  
        } catch (Exception e) {  
            e.printStackTrace();  
            System.out.println("读取" + filename + "出错！");  
        }  
		
	} 
	
	public static void main( String args[] ) {
//		String XmlData = readToString("M_"+org.tools.GetProperties.getKeyValue("System")+"_"+org.tools.GetProperties.getKeyValue("TableNm")+".xml").replace("<ATTRIBUTE NAME=\"Parameter Filename\" VALUE=\"\"/>", "<ATTRIBUTE NAME=\"Parameter Filename\" VALUE=\"$PMRootDir/EDWParam/edw.param\"/>");
//        System.out.println("<ATTRIBUTE NAME=\"Parameter Filename\" VALUE=\"$PMRootDir/EDWParam/edw.param\"/>");
//		writeLog(XmlData);
		ReplaceColumnNm("D:\\workspace\\Uoo\\UpSertMapping.xml");
	}

}
