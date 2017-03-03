package org.tools;

import com.informatica.powercenter.sdk.mapfwk.core.NativeDataTypes;

public class DataTypeTrans {
	
	public static String Trans(String DataType, String DbType){
		String sb = null;
		
		
		if(DbType == "Oracle"){
			switch(DataType.toString().substring(0, DataType.toString().indexOf("(")))
	        {
	        case "VARCHAR2": sb = NativeDataTypes.Oracle.VARCHAR2; break;
	        case "NUMBER": sb = NativeDataTypes.Oracle.NUMBER_PS; break;
	        case "DATE": sb = NativeDataTypes.Oracle.DATE; break;
	        case "BLOB": sb = NativeDataTypes.Oracle.BLOB; break;
	        case "CHAR": sb = NativeDataTypes.Oracle.CHAR; break;
	        case "CLOB": sb = NativeDataTypes.Oracle.CLOB; break;
	        case "LONG": sb = NativeDataTypes.Oracle.LONG; break;
	        case "LONGRAW": sb = NativeDataTypes.Oracle.LONGRAW; break;
	        case "NCHAR": sb = NativeDataTypes.Oracle.NCHAR; break;
	        case "NCLOB": sb = NativeDataTypes.Oracle.NCLOB; break;
	        case "TIMESTAMP": sb = NativeDataTypes.Oracle.TIMESTAMP; break;
	        case "VARCHAR": sb = NativeDataTypes.Oracle.VARCHAR; break;
	        default: sb = NativeDataTypes.Oracle.VARCHAR2; break; 
		
        }; 
		}else if(DbType == "TD"){
//			System.out.println(DataType.toString());
			switch(DataType.toString().substring(0, DataType.toString().indexOf("(")))
	        {
	        case "VARCHAR2": sb = NativeDataTypes.Teradata.VARCHAR; break;
	        case "NUMBER": sb = NativeDataTypes.Teradata.DECIMAL; break;
	        case "DATE": sb = NativeDataTypes.Teradata.TIMESTAMP; break;
	        case "CHAR": sb = NativeDataTypes.Teradata.CHAR; break;
	        case "NCHAR": sb = NativeDataTypes.Teradata.CHAR; break;
	        case "TIMESTAMP": sb = NativeDataTypes.Teradata.TIMESTAMP; break;
	        case "VARCHAR": sb = NativeDataTypes.Teradata.VARCHAR; break;
	        default: sb = NativeDataTypes.Teradata.VARCHAR; break; 
	        };
		}
		return sb;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
