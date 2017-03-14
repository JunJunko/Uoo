import java.util.ArrayList;
import java.util.List;

import org.tools.ExcelUtil;

public class test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		ArrayList<ArrayList<String>> TableConf = ExcelUtil.readXml(org.tools.GetProperties.getKeyValue("ExcelPath"));
		ArrayList<String> a = new ArrayList<String>();
		
		for (int i = 0; i < TableConf.size(); i++){       	
        	a.add(TableConf.get(i).get(1));
        	
		}
		System.out.println("md5("+a.toString().replace("[", "").replace("]", "").replace(",", "||")+")");
	}

}
