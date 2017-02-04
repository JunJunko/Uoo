package org.LoadData;

import java.beans.Statement;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.tools.MysqlConn;

public class LoadCsv {
	
	
	
	private static void  LoadFileData(String path){   
        // get file list where the path has   
        File file = new File(path);   
        // get the folder list   
        File[] array = file.listFiles();   
          
        for(int i=0;i<array.length;i++){   
            if(array[i].isFile()){   
                // only take file name  

            	StringBuffer result = new StringBuffer();
                try{
                    BufferedReader br = new BufferedReader(new FileReader("shares_data//"+array[i].getName()));//构造一个BufferedReader类来读取文件
                    String s = null;
                    while((s = br.readLine())!=null){//使用readLine方法，一次读一行
                        result.append(System.lineSeparator()+s);
                        String value = result.toString().replace("\'", "").replace(",",  "\',\'");
                        

                            String sql = "INSERT INTO getdata VALUES ('"+ value + "\');";
                            Statement statement = null;
                            Connection conn = null;
                            try {
                                conn = MysqlConn.getConn();
                                statement = (Statement) conn.createStatement();
                                int Res = ((java.sql.Statement) statement).executeUpdate(sql);
                                System.out.println(Res > 0 ? "插入数据成功" : "插入数据失败");

                            } catch (Exception e) {
                                e.printStackTrace();
                            } finally {
                            	MysqlConn.Realsase(conn, statement);
                            }

                        }
                    
                    br.close();    
                }catch(Exception e){
                    e.printStackTrace();
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
		LoadFileData("shares_data//");
	}

}
