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

	/**
	 * Create sources
	 */
	protected void createSources() {
		ordersSource = this.CreateCrm(org.tools.GetProperties.getKeyValue("TableNm"), org.tools.GetProperties.getKeyValue("SourceFolder"));
		folder.addSource(ordersSource);
		orderDetailsSource = this.CreateCrm(org.tools.GetProperties.getKeyValue("TableNm"), "test");
		folder.addSource(orderDetailsSource);
	}

	/**
	 * Create targets
	 */
	protected void createTargets() {
		outputTarget = this.createFlatFileTarget("Joiner_Output");
	}

	protected void createMappings() throws Exception {
		mapping = new Mapping("UpSertMapping", "UpSertMapping",
				"This is join sample");
		setMapFileName(mapping);
		TransformHelper helper = new TransformHelper(mapping);

		// Pipeline - 1
		// create DSQ for Order_Details
		RowSet TDDetDSQ = (RowSet) helper.sourceQualifier(orderDetailsSource)
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
//		InputSet ordDetInputSet = new InputSet(ordDetAGG, orderCostContext); // propage
																				// only
																				// OrderCost

		// Pipeline - 2
		// create DSQ for Order
		RowSet ordDSQ = (RowSet) helper.sourceQualifier(ordersSource)
				.getRowSets().get(0);
		
		InputSet ordDetInputSet = new InputSet(TDDetDSQ);
		


		// Join Pipeline-1 to Pipeline-2
		List<InputSet> inputSets = new ArrayList<InputSet>();
		inputSets.add(ordDetInputSet); // collection includes only the detail

		RowSet joinRowSet = (RowSet) helper.join(inputSets,
				new InputSet(ordDSQ), "ROW_ID = IN_ROW_ID",
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
        for (int i = 0; i < ExcelUtil.readXml(org.tools.GetProperties.getKeyValue("ExcelPath")).size(); i++){
        	
        	List<String> a = (List) ExcelUtil.readXml(org.tools.GetProperties.getKeyValue("ExcelPath")).get(i);

        	if (a.get(0).equals(org.tools.GetProperties.getKeyValue("TableNm"))){
//        		
        	    String exp = "String(40,0)"+" "+a.get(1)+"_out"+" = iif(isnull("+a.get(1)+"),"+"IN_"+a.get(1)+","+a.get(1)+")";
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
		
		PortPropagationContext exclOrderCost = PortPropagationContextFactory
				.getContextForExcludeColsFromAll(new String[] { "OrderCost" }); // exclude
																				// OrderCost
																				// while
																				// writing
																				// to
																				// target

		// write to target
		mapping.writeTarget(new InputSet(expRowSet2, exclOrderCost),
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
