package org.tools;

import java.util.Arrays;
import java.util.List;

public class RePlaceOG {

	public static List<String> OG(){
		
		String[] strArray = null;  
        strArray = org.tools.GetProperties.getKeyValue("OGCloumn").toUpperCase().split(",");
        List<String> OGCloumn =  Arrays.asList(strArray);
		return OGCloumn;
		
	}
}
