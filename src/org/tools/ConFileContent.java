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
			FileOutputStream out = new FileOutputStream(file, false); // 如果追加方式用true
			StringBuffer sb = new StringBuffer();
			sb.append(str + "\n");
			out.write(sb.toString().getBytes("GBK"));// 注意需要转换对应的字符集
			out.close();
		} catch (IOException ex) {
			ex.printStackTrace();
			System.out.println(ex.getStackTrace());
		}
		return str;
	}

	public static String ReplaceColumnNm(String filename) {
		StringBuffer Data = new StringBuffer();
		try {
			FileInputStream in = new FileInputStream(filename);
			InputStreamReader inReader = new InputStreamReader(in, "GBK");
			BufferedReader bufReader = new BufferedReader(inReader);
			String line = null;

			String TagReg = ".*TARGETFIELD.*"; // 判断字符串中是否含有TARGETFIELD
			String Tagregex = ".* NAME=\".*?_out\".*?";

			String ConReg = ".*CONNECTOR.*"; // 判断字符串中是否含有CONNECTOR
			String Conregex = "TOFIELD=\"(.*?)_out\"";
			String TransType = ".*FROMINSTANCE=\"UPD_.*";
			while ((line = bufReader.readLine()) != null) {

				if (line.matches(Tagregex) && line.matches(TagReg)) {
					Pattern pattern = Pattern.compile(" NAME=\"(.*?)_out\"");

					Matcher m = pattern.matcher(line);
					if (m.find()) {
						String ReplaceStr = m.group(1);
						// System.out.println("第" + i + "行：" + sourceStrArray);
//						System.out.println(line.replaceAll(" NAME=\".*?_out\"", " NAME=\"" + ReplaceStr + "\""));
						Data.append(line.replaceAll(" NAME=\".*?_out\"", " NAME=\"" + ReplaceStr + "\""));
						Data.append("\n");
					}

				} else if (line.matches(ConReg) && line.matches(TransType)) {
					Pattern pattern = Pattern.compile(Conregex);

					Matcher m1 = pattern.matcher(line);
					if (m1.find()) {
						String ReplaceStr = m1.group(1);
						// System.out.println("第" + i + "行：" + sourceStrArray);
						// System.out.println(line.replaceAll("TOFIELD=\"(.*?)_out\"",
						// "TOFIELD=\""+ReplaceStr+"\""));
//						System.out.println("++++++++++++++++++"+ReplaceStr);
						Data.append(line.replaceAll("TOFIELD=\".*?_out\"", "TOFIELD=\"" + ReplaceStr + "\""));
						Data.append("\n");
					}else{
						Data.append(line + "\n");
					}
					// System.out.println("第" + i + "行：" +line);

				} else {
					Data.append(line + "\n");
				}
			}
			bufReader.close();
			inReader.close();
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("读取" + filename + "出错！");
		}
		return Data.toString().replace("<ATTRIBUTE NAME=\"Parameter Filename\" VALUE=\"\"/>", "<ATTRIBUTE NAME=\"Parameter Filename\" VALUE=\"$PMRootDir/EDWParam/edw.param\"/>")
				.replace("BUSINESSNAME=\"DW_ETL_DT\" DESCRIPTION=\"\" DATATYPE=\"timestamp\" KEYTYPE=\"NOT A KEY\" PRECISION=\"19\"", "BUSINESSNAME=\"DW_ETL_DT\" DESCRIPTION=\"\" DATATYPE=\"date\" KEYTYPE=\"NOT A KEY\" PRECISION=\"10\"")
//				.replace("\"Update else Insert\" VALUE=\"NO", "\"Update else Insert\" VALUE=\"YES")
				.replace("\"Treat source rows as\" VALUE=\"Insert\"", "\"Treat source rows as\" VALUE=\"Data driven\"")
				.replace("NAME=\"Sorter Cache Size\" VALUE=\"8388608\"", "NAME=\"Sorter Cache Size\" VALUE=\"auto\"")
				.replace("Expression DMO Tx\" REUSABLE=\"NO\"", "Expression DMO Tx\" REUSABLE=\"YES\"");

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
		writeLog(ReplaceColumnNm("D:\\workspace\\Uoo\\M_CX_ACCESS_LOG.xml"));
	}

}
