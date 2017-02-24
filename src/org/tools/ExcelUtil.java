package org.tools;
import java.io.ByteArrayOutputStream;  
import java.io.File;  
import java.io.FileInputStream;  
import java.io.FileOutputStream;  
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;  
import java.text.DecimalFormat;  
import java.text.SimpleDateFormat;  
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.poi.hslf.model.Sheet;
import org.apache.poi.hssf.usermodel.HSSFCell;  
import org.apache.poi.hssf.usermodel.HSSFDateUtil;  
import org.apache.poi.hssf.usermodel.HSSFRow;  
import org.apache.poi.hssf.usermodel.HSSFSheet;  
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFCell;  
import org.apache.poi.xssf.usermodel.XSSFRow;  
import org.apache.poi.xssf.usermodel.XSSFSheet;  
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.Source;  
import com.informatica.powercenter.sdk.mapfwk.core.Source;
public class ExcelUtil {  
	
	
    public static void main(String[] args) {  
        System.out.println( readXml("F:\\g��������\\shsnc\\���޼�\\test.xlsx").get(1));

    }  
    
   

	public static  ArrayList readXml(String fileName){  
        boolean isE2007 = false;    //�ж��Ƿ���excel2007��ʽ  
        List TableList = new ArrayList();
        List ColumnList = new ArrayList();
        List TypeList = new ArrayList();
        ArrayList HashList = new ArrayList();
        ArrayList ReList = new ArrayList();
        if(fileName.endsWith("xlsx"))  
            isE2007 = true;  
        try {  
            InputStream input = new FileInputStream(fileName);  //����������  
            Workbook wb  = null;  
            //�����ļ���ʽ(2003����2007)����ʼ��  
            if(isE2007)  
                wb = new XSSFWorkbook(input);  
            else  
                wb = new HSSFWorkbook(input);  
            org.apache.poi.ss.usermodel.Sheet sheet =  wb.getSheetAt(0);     //��õ�һ������  
            Iterator<Row> rows = ((org.apache.poi.ss.usermodel.Sheet) sheet).rowIterator(); //��õ�һ�������ĵ�����  
            int firstRowNum = sheet.getFirstRowNum();  
            int lastRowNum = sheet.getLastRowNum();  
            
            
            
            Row row = null;  
            Cell cell_a = null;  
            Cell cell_b = null;  
            Cell cell_c = null;
            String cellValue;
            String cellValue2;
            String cellValue3;
            HashMap<String, Integer> Hm = new HashMap();
            
            
            for (int i = 1; i <= lastRowNum; i++) {  
             row = sheet.getRow(i);          //ȡ�õ�i��  
             cell_a = row.getCell(0);        //ȡ��i�еĵ�һ��  
             
             cellValue = cell_a.getStringCellValue().trim();  
           
//             System.out.println(cellValue);
//             if (!TableList.contains(cellValue)){
                 TableList.add(cellValue);
//             }  
             
             row = sheet.getRow(i);          //ȡ�õ�i��  
             cell_b = row.getCell(1);        //ȡ��i�еĵ�һ��  
             cellValue2 = cell_b.getStringCellValue().trim();
             ColumnList.add(cellValue2);
             
             
             
             row = sheet.getRow(i);          //ȡ�õ�i��  
             cell_c = row.getCell(3);        //ȡ��i�еĵ�һ��  
             cellValue3 = cell_c.getStringCellValue().trim();
             TypeList.add(cellValue3);
             
             HashList.add(cellValue);
             HashList.add(cellValue2);
             HashList.add(cellValue3);
             
             ReList.add(HashList);
             
             HashList = new ArrayList();
             
             if (Hm.containsKey(cellValue)){
            	 Hm.put(cellValue, Hm.get(cellValue)+1);     	 
             }else{
            	 Hm.put(cellValue, 1);
             }
             
             }  
//            System.out.println(Hm);
//            List listWithoutDup = new ArrayList(new HashSet(TableList));
//            System.out.println(TableList);
//            System.out.println(ColumnList.size());
//            int o = 0;
//            for(int i = 1; i <= TableList.size(); i++){
//            	
//            	for(int j = 0; j < Hm.get(TableList.get(o)); j++){
//            		
//            		System.out.print(j);
//            		System.out.println(ColumnList);
//            		System.out.println(ColumnList.get(j)+"____________"+TypeList.get(j));
//            		
//            		
//            	}
//            	o++;
//            	
////            	return ColumnList;
//            }
            
            
            
//            System.out.println(ColumnList.get(8));
   
            

        } catch (IOException ex) {  
            ex.printStackTrace();  
        }
        
        
        
        
        
		return ReList;
       
        
       
        
    }  
    
    

}