package org.tools;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class UpdateXml {

	private static void updateAttributeValue(String filePath) {
		
		Element SOURCE = null;
		Element SOURCEFIELD = null;
		NodeList SourceLab = null;
		DocumentBuilder dBuilder = null;
		Document doc = null;
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		try {
			dBuilder = dbFactory.newDocumentBuilder();
			File xmlFile = new File(filePath);
			doc = dBuilder.parse(xmlFile);
			doc.getDocumentElement().normalize();
		} catch (ParserConfigurationException | SAXException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		NodeList employees = doc.getElementsByTagName("SOURCE");

		// loop for each employee
		for (int i = 0; i < employees.getLength(); i++) {
			SOURCE = (Element) employees.item(i);

//			System.out.println(SOURCE.getAttribute("DBDNAME"));
			String DBDNAME = SOURCE.getAttribute("DBDNAME");
			if (DBDNAME.equals("ODS")) {
				SourceLab = SOURCE.getElementsByTagName("SOURCEFIELD");
//				 System.out.println(SourceLab);
				for (int j = 0; j < SourceLab.getLength(); j++) {
					SOURCEFIELD = (Element) SourceLab.item(j);
					String name = SOURCEFIELD.getAttribute("NAME");
					if(org.tools.RePlaceOG.OG().contains(name)){
						System.out.println(name);
						SOURCEFIELD.setAttribute("NAME", name+"_OG");
						
					}
//					System.out.println(name);
					
					TransformerFactory transformerFactory = TransformerFactory.newInstance();  
		            Transformer transformer;
					try {
						transformer = transformerFactory.newTransformer();
						doc.getDocumentElement().normalize();
						DOMSource domSource = new DOMSource(doc);  
			            // ÉèÖÃ±àÂëÀàÐÍ  
			            transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");  
			            StreamResult result = new StreamResult(new File(filePath));  
			            transformer.transform(domSource, result);  
					} catch (TransformerConfigurationException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (TransformerException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}  
		            
				}
			}

			// prefix id attribute with M

			// }else{
			// //prefix id attribute with F
			// emp.setAttribute("id", "F"+emp.getAttribute("id"));
			// }
		}

	}

	public static void main(String[] args) {
		
		updateAttributeValue("M_ANSWERNAIRE.xml");

    
    }

}
