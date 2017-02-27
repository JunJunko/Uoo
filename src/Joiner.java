/*
 * Joiner.java Created on Nov 4, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

import java.util.ArrayList;
import java.util.List;

import org.tools.ExcelUtil;

import com.informatica.powercenter.sdk.mapfwk.connection.SourceTargetType;
import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.MappingVariable;
import com.informatica.powercenter.sdk.mapfwk.core.MappingVariableDataTypes;
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
//	protected String System = org.tools.GetProperties.getKeyValue("System");

	/**
	 * Create sources
	 */
	protected void createSources() {
		ordersSource = this.CreateCrm("O_"+org.tools.GetProperties.getKeyValue("System")+"_"+org.tools.GetProperties.getKeyValue("TableNm"), org.tools.GetProperties.getKeyValue("SourceFolder"), "TD");
		folder.addSource(ordersSource);
		orderDetailsSource = this.CreateCrm(org.tools.GetProperties.getKeyValue("TableNm"), org.tools.GetProperties.getKeyValue("SourceFolder"), "Oracle");
		folder.addSource(orderDetailsSource);
	}
	
	

	/**
	 * Create targets
	 */
	protected void createTargets() {
		outputTarget = this.createRelationalTarget( SourceTargetType.Teradata,
                "O_"+org.tools.GetProperties.getKeyValue("System")+"_"+org.tools.GetProperties.getKeyValue("TableNm"));
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



		// Pipeline - 2
//		// 导入源的sourceQualifier
		RowSet SouSQ = (RowSet) helper.sourceQualifier(ordersSource)
				.getRowSets().get(0);
//		
		InputSet SouInputSet = new InputSet(SouSQ);


	   

		// Join Pipeline-1 to Pipeline-2
		List<InputSet> inputSets = new ArrayList<InputSet>();
		inputSets.add(SouInputSet); // collection includes only the detail
		

		
        //将SQ连到Join组件
		RowSet joinRowSet = (RowSet) helper.join(inputSets,
				new InputSet(TagSQ), "ROW_ID = IN_ROW_ID",
				"Join_Order_And_Details").getRowSets().get(0);

		

//		InputSet joinInputSet = new InputSet(joinRowSet);
		

		// Apply expression to calculate TotalOrderCost
		
		List<TransformField> transFields = new ArrayList<TransformField>();
		
		
//		String expr = "integer(1,0) DW_OPER_FLAG = 1";
//        TransformField outField = new TransformField( expr );
//        transFields.add( outField );

        String Column = "";
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

        String exp1 = "integer(1, 0) DW_OPER_FLAG = decode(true,isnull(ROW_ID), 0, ROW_ID = IN_ROW_ID AND LAST_UPD = IN_LAST_UPD,2, 1)";
        String exp2 = "date/time(29, 9) DW_ETL_DT = to_date($$PRVS1D_CUR_DATE,'yyyymmdd')";
        String exp3 = "date/time(29, 9) DW_UPD_TM = decode(true,isnull(ROW_ID), 0, ROW_ID = SESSSTARTTIME";
        TransformField outField1 = new TransformField( exp1 );
        TransformField outField2 = new TransformField( exp2 );
        TransformField outField3 = new TransformField( exp3 );
        transFields.add( outField1 );
        transFields.add( outField2 );
        transFields.add( outField3 );

		TransformField totalOrderCost = null;
//				new TransformField(
//				"decimal(24,0) TotalOrderCost = OrderCost + Freight");
		
		
		
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
		
		
		
		
		
//		List<TransformField> transFields3 = new ArrayList<TransformField>();
//		 for (int i = 0; i < TableConf.size(); i++){
//			 List<String> a = TableConf.get(i);	        	
//	        	if (a.get(0).equals(org.tools.GetProperties.getKeyValue("org.tools.GetProperties.getKeyValue("TableNm")"))){
//	        		
//	        		String sb = null;
//	        		switch(a.get(2).toString().substring(0, a.get(2).toString().indexOf("(")))
//	                {
//	                case "VARCHAR2": sb = a.get(2).replace("VARCHAR2", "String").replace(")", ",0)"); break;
//	                case "NUMBER": sb = a.get(2).replace("NUMBER", "decimal").replace(")", ",0)"); break;
//	                case "DATE": sb = "date/time(29,9)"; break;
//	                case "BLOB": sb = a.get(2).replace("BLOB", "binary").replace(")", ",0)"); break;
//	                case "CHAR": sb = a.get(2).replace("CHAR", "String").replace(")", ",0)"); break;
//	                case "CLOB": sb = a.get(2).replace("CLOB", "binary").replace(")", ",0)"); break;
//	                case "LONG": sb = a.get(2).replace("LONG", "binary").replace(")", ",0)"); break;
//	                case "LONGRAW": sb = a.get(2).replace("LONGRAW", "text").replace(")", ",0)"); break;
//	                case "NCHAR": sb = a.get(2).replace("NCHAR", "String").replace(")", ",0)"); break;
//	                case "NCLOB": sb = a.get(2).replace("NCLOB", "binary").replace(")", ",0)"); break;
//	                case "TIMESTAMP": sb = "date/time(29,9)"; break;
//	                case "VARCHAR": sb = a.get(2).replace("VARCHAR", "String").replace(")", ",0)"); break;
//	                default: sb = "String(50,0)"; break; 
//	                };
////	        		
////	                System.out.println(sb);
//	        	    String exp3 = sb+" "+a.get(1)+"_test"+" = "+a.get(1)+"_out";
//	        	    System.out.println(exp3);
//	        	    TransformField outField = new TransformField( exp3 );
//	                transFields3.add( outField );
//	             
//	                
//	        	}
//	        }
//		 
//
//		    
//			RowSet expRowSet3 = (RowSet) helper.expression(expRowSet2,
//					transFields3, "Expression_Total_Order_Cost2").getRowSets()
//					.get(0);
		
		
		
		RowSet filterRS = (RowSet) helper.updateStrategy( expRowSet2,
                "decode(true, DW_OPER_FLAG == 1, DD_INSERT, DW_OPER_FLAG == 2, DD_REJECT, DD_UPDATE)", "updateStrategy_transform" )
                .getRowSets().get( 0 );

		

		// write to target
		mapping.writeTarget(new InputSet(filterRS, exclOrderID2),
				outputTarget);
		//增加参数
		MappingVariable mappingVar = new MappingVariable( MappingVariableDataTypes.STRING, "0",
                "mapping variable example", true, "$$PRVS1D_CUR_DATE", "20", "0", true );

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
