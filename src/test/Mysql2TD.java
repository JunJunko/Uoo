package test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Mysql2TD {
	
	 public static void readTxtFile(String filePath){
	        try {
	                String encoding="UTF8";
	                File file=new File(filePath);
	                if(file.isFile() && file.exists()){ //�ж��ļ��Ƿ����
	                    InputStreamReader read = new InputStreamReader(
	                    new FileInputStream(file),encoding);//���ǵ������ʽ
	                    BufferedReader bufferedReader = new BufferedReader(read);
	                    String lineTxt = null;
	                    StringBuffer sb = new StringBuffer();
	                    
	                    String regex = "^CREATE TABLE(.*?)$"; 
//	                    // �� Pattern ��� matcher() ��������һ�� Matcher ����
//	                    Matcher m = p.matcher("Kelvin Li and Kelvin Chan are both working in " +
//	            			"Kelvin Chen's KelvinSoftShop company");
	                    String reg=".*create table.*";
	                    while((lineTxt = bufferedReader.readLine()) != null){
//	                        System.out.println(lineTxt);
	                        sb.append(lineTxt+"\r");
	                    }
	                    read.close();
	                    
	                    String[] strr = sb.toString().split(";");
	                    
	                    for(int i = 0; i < strr.length; i++){
	                    	/*oracleת��
	                    	 */
//	                    	System.out.println(strr[i].contains("create table"));
	                    	//ֻ�ѽ��������������Ը���Ϊֻ���index
	                    	if(strr[i].contains("create table")){
	                    	
	                    		String a = strr[i].replace("VARCHAR2", "VARCHAR").replace("DATE", "TIMESTAMP(6)").replace("NUMBER", "DECIMAL").replace("VARCHAR(3999", "VARCHAR(4000)").replace(" CHAR)", ")").replace("sysdate", "CURRENT_TIMESTAMP(0)").replaceAll("prompt", "").replace("create table ", "CREATE MULTISET TABLE ODS_DDL.").replace("\r(", ",NO FALLBACK ,\r"+
	                    				"     NO BEFORE JOURNAL,\r"+
	                    				"     NO AFTER JOURNAL,\r"+
	                    				"     CHECKSUM = DEFAULT,\r"+
	                    				"     DEFAULT MERGEBLOCKRATIO\r"+
	                    				"     (\r");
	                    	    method2("a.sql", a+";");
	                    	
	                    	}
	                    	
	                    	
	                    }
	        }else{
	            System.out.println("�Ҳ���ָ�����ļ�");
	        }
	        } catch (Exception e) {
	            System.out.println("��ȡ�ļ����ݳ���");
	            e.printStackTrace();
	        }
	     
	    }
	 
	 


public static void method2(String file, String conent) {
BufferedWriter out = null;
try {
out = new BufferedWriter(new OutputStreamWriter(
new FileOutputStream(file, true)));
out.write(conent+"\r\n");
} catch (Exception e) {
e.printStackTrace();
} finally {
try {
out.close();
} catch (IOException e) {
e.printStackTrace();
}
}
}



	public static void main(String[] args) {
		// TODO Auto-generated method stub
		readTxtFile("F:\\g��������\\shsnc\\���޼�\\�������\\�ҳϹ˿͹���ϵͳ.sql");
		String str5 = "                                                                      "+
				"                                                                      "+
				"                                                                      "+
				"Creating table CX_FXH_BUYPV                                           "+
				"===========================                                           "+
				"                                                                      "+
				"create table CX_FXH_BUYPV                                              "+
				"                                                                      "+
				" row_id           VARCHAR(15 ) not null,                              "+
				" created          TIMESTAMP(6) default CURRENT_TIMESTAMP(0) not null, "+
				" created_by       VARCHAR(15 ) not null,                              "+
				" last_upd         TIMESTAMP(6) default CURRENT_TIMESTAMP(0) not null, "+
				" last_upd_by      VARCHAR(15 ) not null,                              "+
				" modification_num DECIMAL(10) default 0 not null,                     "+
				" conflict_id      VARCHAR(15 ) default '0' not null,                  "+
				" db_last_upd      TIMESTAMP(6),                                       "+
				" pv_date          TIMESTAMP(6),                                       "+
				" pv_money         DECIMAL(10),                                        "+
				" pv_num           DECIMAL(10),                                        "+
				" act_name         VARCHAR(50 ),                                       "+
				" card_name        VARCHAR(50 ),                                       "+
				" card_num         VARCHAR(15 ),                                       "+
				" card_phone       VARCHAR(15 ),                                       "+
				" db_last_upd_src  VARCHAR(50 ),                                       "+
				" reffer_num       VARCHAR(15 ),                                       "+
				" pv_month         VARCHAR(15 )                                        "+
				"                                                                      "+
				"                                                                      "+
				"                                                                      ";
		 System.out.println(str5.matches(".*"));
	}

}