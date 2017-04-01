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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

	public static String writeLog(String str) {
		try {
			String path = "xml\\M_" + org.tools.GetProperties.getKeyValue("System") + "_"
					+ org.tools.GetProperties.getKeyValue("TableNm").toUpperCase() + ".xml";
			File file = new File(path);
			if (!file.exists())
				file.createNewFile();
			FileOutputStream out = new FileOutputStream(file, false); // ���׷�ӷ�ʽ��true
			StringBuffer sb = new StringBuffer();
			sb.append(str + "\n");
			out.write(sb.toString().getBytes("GBK"));// ע����Ҫת����Ӧ���ַ���
			out.close();
		} catch (IOException ex) {
			ex.printStackTrace();
			System.out.println(ex.getStackTrace());
		}
		return str;
	}

	public static String ReplaceColumnNm(String filename) {
		StringBuffer Data = new StringBuffer();
		org.tools.UpdateXml.updateAttributeValue(filename);
		try {
			FileInputStream in = new FileInputStream(filename);
			InputStreamReader inReader = new InputStreamReader(in, "GBK");
			BufferedReader bufReader = new BufferedReader(inReader);
			String line = null;

			String TagReg = ".*TARGETFIELD.*"; // �ж��ַ������Ƿ���TARGETFIELD
			String Tagregex = ".* NAME=\".*?\".*?";

			String ConReg = ".*CONNECTOR.*"; // �ж��ַ������Ƿ���CONNECTOR
			String Conregex = "TOFIELD=\"(.*?)_out\"";
			String TransType = ".*FROMINSTANCE=\"FIT_.*";
			String ExpType = ".*FROMINSTANCETYPE=\"Update Strategy\".*"; 

			
			while ((line = bufReader.readLine()) != null) {

				if (line.matches(TagReg) && line.matches(Tagregex)) {
					// System.out.println(line.se);
					Pattern pattern = Pattern.compile(" NAME=\"(.*?)\"");

					// Pattern patternOut = Pattern.compile("
					// NAME=\"(.*?)_out2\"");

					Matcher m = pattern.matcher(line);
					// Matcher mo = pattern.matcher(line);

					if (m.find()) {
						String ReplaceStr = m.group(1).replace("_out", "");
//						 System.out.println(ReplaceStr);
						System.out.println(ReplaceStr);
						if (org.tools.RePlaceOG.OG().contains(ReplaceStr.replace("_out", ""))) {
							 
							Data.append(line.replaceAll(" NAME=\".*?\"", " NAME=\"" + ReplaceStr.replace("_out", "") + "_OG" + "\""));
							Data.append("\n");
						} else {
//							 System.out.println(ReplaceStr);
							Data.append(line.replaceAll(" NAME=\".*?_out\"", " NAME=\"" + ReplaceStr.replace("_out", "") + "\""));
							Data.append("\n");
						}
						// }
					}
					// else if(mo.find()){
					// String ReplaceStr = mo.group(1);
					// System.out.println(ReplaceStr+"SSS");
					//
					// Data.append(line.replaceAll(" NAME=\".*?_out2\"", "
					// NAME=\"" + ReplaceStr + "\""));
					// Data.append("\n");
					// }

				} else if (line.matches(ConReg) && line.matches(TransType)) {
					Pattern pattern = Pattern.compile(Conregex);

					Matcher m1 = pattern.matcher(line);
					if (m1.find()) {
						String ReplaceStr = m1.group(1);

						// System.out.println("��" + i + "�У�" + sourceStrArray);
						// System.out.println(line.replaceAll("TOFIELD=\"(.*?)_out2\"",
						// "TOFIELD=\""+ReplaceStr+"\""));
						// System.out.println("++++++++++++++++++"+ReplaceStr);
						if (org.tools.RePlaceOG.OG().contains(ReplaceStr)) {
							Data.append(line.replaceAll(" TOFIELD=\".*?\"", " TOFIELD=\"" + ReplaceStr + "_OG" + "\""));
							Data.append("\n");
						}else{
						Data.append(line.replaceAll("TOFIELD=\".*?_out\"", "TOFIELD=\"" + ReplaceStr + "\""));
						Data.append("\n");
						}
					} else {
						Data.append(line + "\n");
					}
					// System.out.println("��" + i + "�У�" +line);

				} 
//				else if (line.matches(ConReg) && line.matches(ExpType)) {
//
//					Pattern pattern = Pattern.compile(" TOFIELD=\"(.*?)\"");
//					Matcher m = pattern.matcher(line);
//					if (m.find()) {
//						String ReplaceStr = m.group(1);
//
//						if (org.tools.RePlaceOG.OG().contains(ReplaceStr)) {
//							Data.append(line.replaceAll(" TOFIELD=\".*?\"", " TOFIELD=\"" + ReplaceStr + "_OG" + "\""));
//							Data.append("\n");
//						}else{
//							Data.append(line + "\n");
//						}
//					}
//
//				}
				else {
					Data.append(line + "\n");
				}
			}
			bufReader.close();
			inReader.close();
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("��ȡ" + filename + "����");
		}
		return Data.toString()
				.replace("<ATTRIBUTE NAME=\"Parameter Filename\" VALUE=\"\"/>",
						"<ATTRIBUTE NAME=\"Parameter Filename\" VALUE=\"$PMRootDir/EDWParam/edw.param\"/>")
				.replace(
						"BUSINESSNAME=\"DW_ETL_DT\" DESCRIPTION=\"\" DATATYPE=\"timestamp\" KEYTYPE=\"NOT A KEY\" PRECISION=\"19\"",
						"BUSINESSNAME=\"DW_ETL_DT\" DESCRIPTION=\"\" DATATYPE=\"date\" KEYTYPE=\"NOT A KEY\" PRECISION=\"10\"")
//				.replace("\"Update else Insert\" VALUE=\"NO", "\"Update else Insert\" VALUE=\"YES")
//				.replace("\"Treat source rows as\" VALUE=\"Insert\"", "\"Treat source rows as\" VALUE=\"Data driven\"")
				.replace("NAME=\"Sorter Cache Size\" VALUE=\"8388608\"", "NAME=\"Sorter Cache Size\" VALUE=\"auto\"")
				.replace("<POWERMART", "<!DOCTYPE POWERMART SYSTEM \"powrmart.dtd\"><POWERMART")
		// .replace("Expression DMO Tx\" REUSABLE=\"NO\"", "Expression DMO Tx\"
		// REUSABLE=\"YES\"")
		;

	}

	public static void main(String args[]) {
		// String XmlData =
		// readToString("M_"+org.tools.GetProperties.getKeyValue("System")+"_"+org.tools.GetProperties.getKeyValue("TableNm")+".xml").replace("<ATTRIBUTE
		// NAME=\"Parameter Filename\" VALUE=\"\"/>", "<ATTRIBUTE
		// NAME=\"Parameter Filename\"
		// VALUE=\"$PMRootDir/EDWParam/edw.param\"/>");
		// System.out.println("<ATTRIBUTE NAME=\"Parameter Filename\"
		// VALUE=\"$PMRootDir/EDWParam/edw.param\"/>");
		// writeLog(XmlData);
		ReplaceColumnNm("D:\\workspace\\Uoo\\M_ACTIVITY_SHARE.xml");
	}

}
