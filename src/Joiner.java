/*
 * Joiner.java Created on Nov 4, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.tools.ExcelUtil;

import com.informatica.powercenter.sdk.mapfwk.connection.SourceTargetType;
import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.NativeDataTypes;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformField;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;
import com.informatica.powercenter.sdk.mapfwk.portpropagation.PortPropagationContext;
import com.informatica.powercenter.sdk.mapfwk.portpropagation.PortPropagationContextFactory;

/**
 * 
 * 
 */
public class Joiner extends Base {
	protected Target outputTarget;

	protected Source ordersSource;

	protected Source orderDetailsSource;
	
	protected ArrayList<ArrayList<String>> TableConf = ExcelUtil.readXml(org.tools.GetProperties.getKeyValue("ExcelPath"));
	protected String TableNm = org.tools.GetProperties.getKeyValue("TableNm");

	/**
	 * Create sources
	 */
	protected void createSources() {
		ordersSource = this.CreateCrm(TableNm, org.tools.GetProperties.getKeyValue("SourceFolder"));
		folder.addSource(ordersSource);
		orderDetailsSource = this.CreateCrm(TableNm, "test");
		folder.addSource(orderDetailsSource);
	}
	
	

	/**
	 * Create targets
	 */
	protected void createTargets() {
		outputTarget = this.createRelationalTarget( SourceTargetType.Teradata,
                "O_"+org.tools.GetProperties.getKeyValue("System")+"_"+org.tools.GetProperties.getKeyValue("TableNm") );
	}

	protected void createMappings() throws Exception {
		mapping = new Mapping("UpSertMapping", "UpSertMapping",
				"This is join sample");
		setMapFileName(mapping);
		TransformHelper helper = new TransformHelper(mapping);

		// Pipeline - 1
		// 导入目标的sourceQualifier
		RowSet TagSQ = (RowSet) helper.sourceQualifier(orderDetailsSource)
				.getRowSets().get(0);

		// calculate order cost using the formula
		// SUM((UnitPrice * Quantity) * (100 - Discount1) / 100) grouped by
		// OrderId
//		TransformField orderCost = new TransformField(
//				"decimal(24,0) OrderCost = (SUM((UnitPrice * Quantity) * (100 - Discount) / 100))");
//		RowSet ordDetAGG = (RowSet) helper.aggregate(ordDetDSQ, orderCost,
//				new String[] { "OrderID" }, "Calculate_Order_Cost")
//				.getRowSets().get(0);
//		PortPropagationContext orderCostContext = PortPropagationContextFactory
//				.getContextForIncludeCols(new String[] { "OrderCost", "OrderID" });
//		InputSet SouInputSet = new InputSet(ordDetAGG, orderCostContext); // propage
																				// only
																				// OrderCost

		// Pipeline - 2
		// 导入源的sourceQualifier
		RowSet SouSQ = (RowSet) helper.sourceQualifier(ordersSource)
				.getRowSets().get(0);
		
		InputSet TagInputSet = new InputSet(TagSQ);
		InputSet SouInputSet = new InputSet(SouSQ);
		
		
		 
		
		
		//将两个sourceQualifier通过express组件重命名
		List<TransformField> TagFields = new ArrayList<TransformField>();
		List<TransformField> SouFields = new ArrayList<TransformField>();
		 for (int i = 0; i < TableConf.size(); i++){
	        	
	        	List<String> a = TableConf.get(i);

	        	if (a.get(0).equals(org.tools.GetProperties.getKeyValue("TableNm"))){
	        		
	        		String sb = null;
	        		switch(a.get(2).toString().substring(0, a.get(2).toString().indexOf("(")))
	                {
	                case "VARCHAR2": sb = a.get(2).replace("VARCHAR2", "String").replace(")", ",0)"); break;
	                case "NUMBER": sb = a.get(2).replace("NUMBER", "decimal").replace(")", ",0)"); break;
	                case "DATE": sb = "date/time(29,9)"; break;
	                case "BLOB": sb = a.get(2).replace("BLOB", "binary").replace(")", ",0)"); break;
	                case "CHAR": sb = a.get(2).replace("CHAR", "String").replace(")", ",0)"); break;
	                case "CLOB": sb = a.get(2).replace("CLOB", "binary").replace(")", ",0)"); break;
	                case "LONG": sb = a.get(2).replace("LONG", "binary").replace(")", ",0)"); break;
	                case "LONGRAW": sb = a.get(2).replace("LONGRAW", "text").replace(")", ",0)"); break;
	                case "NCHAR": sb = a.get(2).replace("NCHAR", "String").replace(")", ",0)"); break;
	                case "NCLOB": sb = a.get(2).replace("NCLOB", "binary").replace(")", ",0)"); break;
	                case "TIMESTAMP": sb = "date/time(29,9)"; break;
	                case "VARCHAR": sb = a.get(2).replace("VARCHAR", "String").replace(")", ",0)"); break;
	                default: sb = "String(50,0)"; break; 
	                };
//	        		
//	                System.out.println(sb);
	        	    String exp_t = sb+" "+a.get(1)+"_t"+" = "+a.get(1);
	        	    String exp_s = sb+" "+a.get(1)+"_s"+" = "+a.get(1);
	        	    TransformField outField_t = new TransformField( exp_t );
	        	    TransformField outField_s = new TransformField( exp_s );
	        	    
	        	    TagFields.add( outField_t );
	        	    SouFields.add( outField_s );
	                
	        	}
	        	System.out.println(a);
		       
		       
		 }   
		 RowSet TagExp = (RowSet) helper.expression(TagSQ, TagFields, "TagReNameExp").getRowSets().get(0);
	     RowSet SouExp = (RowSet) helper.expression(SouSQ, SouFields, "SouReNameExp").getRowSets().get(0);      
		


		// Join Pipeline-1 to Pipeline-2
		List<InputSet> inputSets_s = new ArrayList<InputSet>();
		inputSets_s.add(SouInputSet); // collection includes only the detail
		
		List<InputSet> inputSets_t = new ArrayList<InputSet>();
		inputSets_t.add(SouInputSet); // collection includes only the detail

		RowSet joinRowSet = (RowSet) helper.join(inputSets_s,
				new InputSet(TagSQ), "ROW_ID = IN_ROW_ID",
				"Join_Order_And_Details").getRowSets().get(0);

		

//		InputSet joinInputSet = new InputSet(joinRowSet);
		

		// Apply expression to calculate TotalOrderCost
		
		List<TransformField> transFields = new ArrayList<TransformField>();
		
		
//		String expr = "integer(1,0) DW_OPER_FLAG = 1";
//        TransformField outField = new TransformField( expr );
//        transFields.add( outField );
        String len = null;
        String precision = null;
        String Column = "";
        for (int i = 0; i < TableConf.size(); i++){       	
        	List<String> a = TableConf.get(i); 
        	if (a.get(0).equals(TableNm)){
        		
        		String sb = null;
        		switch(a.get(2).toString().substring(0, a.get(2).toString().indexOf("(")))
                {
                case "VARCHAR2": sb = a.get(2).replace("VARCHAR2", "String").replace(")", ",0)"); break;
                case "NUMBER": sb = a.get(2).replace("NUMBER", "decimal").replace(")", ",0)"); break;
                case "DATE": sb = "date/time(29,9)"; break;
                case "BLOB": sb = a.get(2).replace("BLOB", "binary").replace(")", ",0)"); break;
                case "CHAR": sb = a.get(2).replace("CHAR", "String").replace(")", ",0)"); break;
                case "CLOB": sb = a.get(2).replace("CLOB", "binary").replace(")", ",0)"); break;
                case "LONG": sb = a.get(2).replace("LONG", "binary").replace(")", ",0)"); break;
                case "LONGRAW": sb = a.get(2).replace("LONGRAW", "text").replace(")", ",0)"); break;
                case "NCHAR": sb = a.get(2).replace("NCHAR", "String").replace(")", ",0)"); break;
                case "NCLOB": sb = a.get(2).replace("NCLOB", "binary").replace(")", ",0)"); break;
                case "TIMESTAMP": sb = "date/time(29,9)"; break;
                case "VARCHAR": sb = a.get(2).replace("VARCHAR", "String").replace(")", ",0)"); break;
                default: sb = "String(50,0)"; break; 
                };
//        		
//                System.out.println(sb);
        	    String exp = sb +" "+a.get(1)+"_out"+" = iif(isnull("+a.get(1)+"),"+"IN_"+a.get(1)+","+a.get(1)+")";
        	    TransformField outField = new TransformField( exp );
                transFields.add( outField );
                if(a.get(1) != null){
                	if(Column == ""){
                		Column = a.get(1)+","+"IN_"+a.get(1);
                	}else{
                	    Column = Column+","+a.get(1)+","+"IN_"+a.get(1);
                	}
//                	System.out.println(Column);
                }
                
        	}
        }
//        String[] toBeStored = Column.toArray(new String[Column.size()]);   

        
		TransformField totalOrderCost = new TransformField(
				"decimal(24,0) TotalOrderCost = OrderCost + Freight");
		
		RowSet expRowSet = (RowSet) helper.expression(joinRowSet,
				transFields, "Expression_Total_Order_Cost").getRowSets()
				.get(0);
		

   
        String[] strArray = null;
        strArray = Column.split(",");
        
        PortPropagationContext exclOrderID2 = PortPropagationContextFactory
				.getContextForExcludeColsFromAll(strArray); // exclude
																					// OrderCost
																					// while
																					// writing
																					// to
																					// target
		

        InputSet joinInputSet2 = new InputSet(expRowSet, exclOrderID2);
		
		RowSet expRowSet2 = (RowSet) helper.expression(joinInputSet2,
				totalOrderCost, "Expression_Total_Order_Cost1").getRowSets()
				.get(0);
		
		
		
		
		
		List<TransformField> transFields3 = new ArrayList<TransformField>();
		 for (int i = 0; i < TableConf.size(); i++){
			 List<String> a = TableConf.get(i);	        	
	        	if (a.get(0).equals(org.tools.GetProperties.getKeyValue("TableNm"))){
	        		
	        		String sb = null;
	        		switch(a.get(2).toString().substring(0, a.get(2).toString().indexOf("(")))
	                {
	                case "VARCHAR2": sb = a.get(2).replace("VARCHAR2", "String").replace(")", ",0)"); break;
	                case "NUMBER": sb = a.get(2).replace("NUMBER", "decimal").replace(")", ",0)"); break;
	                case "DATE": sb = "date/time(29,9)"; break;
	                case "BLOB": sb = a.get(2).replace("BLOB", "binary").replace(")", ",0)"); break;
	                case "CHAR": sb = a.get(2).replace("CHAR", "String").replace(")", ",0)"); break;
	                case "CLOB": sb = a.get(2).replace("CLOB", "binary").replace(")", ",0)"); break;
	                case "LONG": sb = a.get(2).replace("LONG", "binary").replace(")", ",0)"); break;
	                case "LONGRAW": sb = a.get(2).replace("LONGRAW", "text").replace(")", ",0)"); break;
	                case "NCHAR": sb = a.get(2).replace("NCHAR", "String").replace(")", ",0)"); break;
	                case "NCLOB": sb = a.get(2).replace("NCLOB", "binary").replace(")", ",0)"); break;
	                case "TIMESTAMP": sb = "date/time(29,9)"; break;
	                case "VARCHAR": sb = a.get(2).replace("VARCHAR", "String").replace(")", ",0)"); break;
	                default: sb = "String(50,0)"; break; 
	                };
//	        		
//	                System.out.println(sb);
	        	    String exp3 = sb+" "+a.get(1)+"_test"+" = "+a.get(1)+"_out";
	        	    System.out.println(exp3);
	        	    TransformField outField = new TransformField( exp3 );
	                transFields3.add( outField );
	             
	                
	        	}
	        }
		 

		    
			RowSet expRowSet3 = (RowSet) helper.expression(expRowSet2,
					transFields3, "Expression_Total_Order_Cost2").getRowSets()
					.get(0);

		

		// write to target
		mapping.writeTarget(new InputSet(expRowSet3, exclOrderID2),
				outputTarget);

		// add mapping to folder
		folder.addMapping(mapping);
		 
	}

	/**
	 * Create workflow method
	 */
	protected void createWorkflow() throws Exception {

		workflow = new Workflow("Workflow_for_Joiner", "Workflow_for_joiner",
				"This workflow for joiner");
		workflow.addSession(session);
		folder.addWorkFlow(workflow);

	}

	public static void main(String args[]) {
		try {
			Joiner joinerTrans = new Joiner();
			if (args.length > 0) {
				if (joinerTrans.validateRunMode(args[0])) {
					joinerTrans.execute();
				}
			} else {
				joinerTrans.printUsage();
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Exception is: " + e.getMessage());
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createSession()
	 */
	protected void createSession() throws Exception {
		session = new Session("Session_For_Joiner", "Session_For_Joiner",
				"This is session for joiner");
		session.setMapping(this.mapping);

	}
}
