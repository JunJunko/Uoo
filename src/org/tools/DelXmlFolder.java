package org.tools;

import java.io.File;

public class DelXmlFolder {
	
	private void clearFiles(String workspaceRootPath){
	     File file = new File(workspaceRootPath);
	     if(file.exists()){
	          deleteFile(file);
	     }
	}  
	
	private void deleteFile(File file){
	     if(file.isDirectory()){
	          File[] files = file.listFiles();
	          for(int i=0; i<files.length; i++){
	               deleteFile(files[i]);
	          }
	     }
	     file.delete();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		clearFiles(null);
	}

}
