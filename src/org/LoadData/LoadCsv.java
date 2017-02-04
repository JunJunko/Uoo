package org.LoadData;

import java.beans.Statement;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;


public class LoadCsv {
	
	
	
	private static void  LoadFileData(String path) throws IOException{   
        // get file list where the path has   
        File file = new File(path);   
        // get the folder list   
        File[] array = file.listFiles();  
        Statement stmt = null;
          
        for(int i=0;i<array.length;i++){   
            if(array[i].isFile()){   
                // only take file name  

            	String result = new String();

                 

						  FileInputStream fis = new FileInputStream("shares_data//"+array[i].getName());   
						  InputStreamReader isr = new InputStreamReader(fis, "GBK");
						  
						  BufferedReader br = new BufferedReader(isr);  
						
						
					
                    String s = null;
                    int rows = 1;
                    
						while((s = br.readLine())!=null){//使用readLine方法，一次读一行
							
						    result = (System.lineSeparator()+s);
						    String value = result.toString().replace("\'", "").replace(",",  "\',\'");
						    
						    String sql = "INSERT INTO getdata VALUES ('"+ value + "\');";
						    System.out.println(rows);
						    if(rows != 1){
						        try {
						        	System.out.println(sql);
								    org.tools.MysqlConn.Insert(sql);
							    } catch (IOException e) {
								// TODO Auto-generated catch block
								    e.printStackTrace();
							    }
						    }
						    
						    rows++;
						                        
						}
					 }
    }  
       
}
	
//	private static int insert(String student) {
//	    Connection conn = getConn();
//	    int i = 0;
//	    String sql = "insert into students (Name,Sex,Age) values(?,?,?)";
//	    PreparedStatement pstmt;
//	    try {
//	        pstmt = (PreparedStatement) conn.prepareStatement(sql);
//	        pstmt.setString(1, student.getName());
//	        pstmt.setString(2, student.getSex());
//	        pstmt.setString(3, student.getAge());
//	        i = pstmt.executeUpdate();
//	        pstmt.close();
//	        conn.close();
//	    } catch (SQLException e) {
//	        e.printStackTrace();
//	    }
//	    return i;
//	}
	
	public static void Loaddata(){
		
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			LoadFileData("shares_data//");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
