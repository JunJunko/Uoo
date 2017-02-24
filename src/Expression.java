/*
 * Expression.java Created on Nov 4, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.informatica.powercenter.sdk.mapfwk.connection.ConnectionInfo;
import com.informatica.powercenter.sdk.mapfwk.connection.ConnectionProperties;
import com.informatica.powercenter.sdk.mapfwk.connection.ConnectionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.connection.SourceTargetType;
import com.informatica.powercenter.sdk.mapfwk.core.DSQTransformation;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.MappingVariable;
import com.informatica.powercenter.sdk.mapfwk.core.MappingVariableDataTypes;
import com.informatica.powercenter.sdk.mapfwk.core.OutputSet;
import com.informatica.powercenter.sdk.mapfwk.core.ParameterFile;
import com.informatica.powercenter.sdk.mapfwk.core.ParameterFileIterator;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.SessionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformField;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;
import com.informatica.powercenter.sdk.mapfwk.samples.SessionProperties;



/**
 * This example applies a simple expression transformation on the Employee table
 * and writes to a target
 * 
 */
public class Expression extends Base {
    // /////////////////////////////////////////////////////////////////////////////////////
    // Instance variables
    // /////////////////////////////////////////////////////////////////////////////////////
    protected Source employeeSrc;
    protected Target TdTarget;

    /**
     * Create sources
     */
    protected void createSources() {
//        employeeSrc = this.createEmployeeSource();
    	employeeSrc = this.CreateCrm(org.tools.GetProperties.getKeyValue("TableNm"), org.tools.GetProperties.getKeyValue("SourceFolder"));
//    	employeeSrc = this.createMysqlSource(org.tools.GetProperties.getKeyValue("TableNm"), org.tools.GetProperties.getKeyValue("SourceFolder"));
        folder.addSource( employeeSrc );
    }

    /**
     * Create targets
     */
    protected void createTargets() {
    	TdTarget =  this.createRelationalTarget( SourceTargetType.Teradata,
                "O_"+org.tools.GetProperties.getKeyValue("System")+"_"+org.tools.GetProperties.getKeyValue("TableNm") );
    }

    public void createMappings() throws Exception {
        // create a mapping
        mapping = new Mapping( "m_"+org.tools.GetProperties.getKeyValue("System")+"_"+org.tools.GetProperties.getKeyValue("TableNm"), "mapping", "Testing Expression sample" );
        setMapFileName( mapping );
        TransformHelper helper = new TransformHelper( mapping );
        // creating DSQ Transformation
        OutputSet outSet = helper.sourceQualifier( employeeSrc );
        RowSet dsqRS = (RowSet) outSet.getRowSets().get( 0 );
        // create an expression Transformation
        // the fields LastName and FirstName are concataneted to produce a new
        // field fullName
        
//        String expr = "integer(1, 0) DW_OPER_FLAG= 1";
//        TransformField outField = new TransformField( expr );
//        String expr2 = "data/time DW_ETL_DT= $$PRVS1D_CUR_DATE";
//        TransformField outField2 = new TransformField( expr2 );
//        String expr3 = "data/time DW_UPD_TM= sysdate";
//        TransformField outField3 = new TransformField( expr3 );
        
        String expr = "integer(1,0) DW_OPER_FLAG = 1";
        TransformField outField = new TransformField( expr );
        
        String expr2 = "date/time(29,9) DW_ETL_DT= to_date($$PRVS1D_CUR_DATE, 'yyyymmdd')";
        TransformField outField2 = new TransformField( expr2 );
        
        String expr3 = "date/time(29,9) DW_UPD_TM= sysdate";
        TransformField outField3 = new TransformField( expr3 );
        
        List<TransformField> transFields = new ArrayList<TransformField>();
        transFields.add( outField );
        transFields.add( outField2 );
        transFields.add( outField3 );
        RowSet expRS = (RowSet) helper.expression( dsqRS, transFields, "exp_transform" ).getRowSets()
                .get( 0 );
//        expRS.getField("YEAR_out").setName("YEAR11111");
        // write to target
        mapping.writeTarget( expRS, TdTarget );
        MappingVariable mappingVar = new MappingVariable( MappingVariableDataTypes.STRING, "0",
                "mapping variable example", true, "$$PRVS1D_CUR_DATE", "20", "0", true );
        mapping.addMappingVariable( mappingVar );
        folder.addMapping( mapping );
    }

    public static void main( String args[] ) {
    	
    	
        try {
            Expression expressionTrans = new Expression();
            if (args.length > 0) {
                if (expressionTrans.validateRunMode( args[0] )) {
                	ArrayList<String> a = GetTableList();
                	for(int i = 0; i < a.size(); i++){
                		org.tools.GetProperties.writeProperties("TableNm", a.get(i));
                        expressionTrans.execute();
                	}
                }
            } else {
                expressionTrans.printUsage();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println( "Exception is: " + e.getMessage() );
        }
    	
    	System.out.println(GetTableList());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createSession()
     */
//    protected void createSession() throws Exception {
//        session = new Session( "Session_For_Expression", "Session_For_Expression",
//                "This is session for expression" );
//        session.setMapping( this.mapping );
//    }
   
    
    
    protected void createSession() throws Exception {
		// TODO Auto-generated method stub
	session = new Session("s_"+org.tools.GetProperties.getKeyValue("System")+"_"+org.tools.GetProperties.getKeyValue("TableNm"), "s_"+org.tools.GetProperties.getKeyValue("System")+"_"+org.tools.GetProperties.getKeyValue("TableNm"),
		"This is session for Expression DMO Tx");
	session.setMapping(this.mapping);
	
	//Adding Connection Objects for substitution mask option
	ConnectionInfo info = new ConnectionInfo(SourceTargetType.Oracle);
	ConnectionProperties cprops = info.getConnProps();
	cprops.setProperty(ConnectionPropsConstants.CONNECTIONNAME, "Oracle");
	cprops.setProperty(ConnectionPropsConstants.CONNECTIONNUMBER, "1");

	
	ConnectionInfo info2 = new ConnectionInfo(SourceTargetType.Oracle);
	ConnectionProperties cprops2 = info2.getConnProps();
	cprops2.setProperty(ConnectionPropsConstants.CONNECTIONNAME, "Oracle");
	cprops2.setProperty(ConnectionPropsConstants.CONNECTIONNUMBER, "2");
	List<ConnectionInfo> cons = new ArrayList<ConnectionInfo>();
	cons.add(info);
	cons.add(info2);
//	session.addConnectionInfosObject(dmo, cons);
	
	//Overriding source connection in Seesion level
	ConnectionInfo newSrcCon = new ConnectionInfo(SourceTargetType.Oracle);
	newSrcCon.setConnectionVariable("$DBConnection_CRM");
//	ConnectionProperties newSrcConprops = newSrcCon.getConnProps();
//	newSrcConprops.setProperty(ConnectionPropsConstants.CONNECTIONNAME, "$DBConnection_CRM");
//	DSQTransformation dsq = (DSQTransformation)mapping.getTransformation("SQ_"+org.tools.GetProperties.getKeyValue("TableNm"));
	DSQTransformation dsq = (DSQTransformation)mapping.getTransformation("SQ_"+org.tools.GetProperties.getKeyValue("TableNm"));
	session.addConnectionInfoObject(dsq, newSrcCon);
	//session.addConnectionInfoObject(jobSourceObj, newSrcCon);
	
	//Overriding target connection in Seesion level
	ConnectionInfo newTgtCon = new ConnectionInfo(SourceTargetType.Teradata_PT_Connection);

	ConnectionProperties newTgtConprops = newTgtCon.getConnProps();
	
	newTgtConprops.setProperty(ConnectionPropsConstants.TRUNCATE_TABLE, "YES");
	
	newTgtConprops.setProperty(SessionPropsConstants.PARAMETER_FILENAME, "$PMRootDir/EDWParam/edw.param");
	
	newTgtCon.setConnectionVariable("$DBConnection_TD");
//	ConnectionProperties newTgtConprops = newTgtCon.getConnProps();
//	newTgtConprops.setProperty( ConnectionPropsConstants.CONNECTIONNAME, "$DBConnection_TD");


	session.addConnectionInfoObject(TdTarget, newTgtCon);
	//Setting session level property.
	Properties props = new Properties();
//	session.addSessionTransformInstanceProperties(dmo, props);

	}

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createWorkflow()
     */
    protected void createWorkflow() throws Exception {
        workflow = new Workflow( "wf_"+org.tools.GetProperties.getKeyValue("System")+"_"+org.tools.GetProperties.getKeyValue("TableNm"), "wf_"+org.tools.GetProperties.getKeyValue("System")+"_"+org.tools.GetProperties.getKeyValue("TableNm"),
                "This workflow for expression" );
//        workflow.setParentFolder(folder);
//        List<String> listOfParams = workflow.getListOfParameters();
//        ParameterFile pmFile = new ParameterFile("$PMRootDir/EDWParam/edw.param");
//        Iterator<String> listOfParamsIter = listOfParams.iterator();
//		int i=0;
//		while(listOfParamsIter.hasNext())
//		{
//			pmFile.setParameterValue(listOfParamsIter.next(), new Integer(i).toString());
//			i++;
//		}
//		pmFile.save();
//		ParameterFileIterator iter = pmFile.iterator();
        workflow.addSession( session );
        workflow.assignIntegrationService(org.tools.GetProperties.getKeyValue("Integration"), org.tools.GetProperties.getKeyValue("Domain"));
        folder.addWorkFlow( workflow );
    }
    
    
    
    public static ArrayList<String> GetTableList() {   
    	ArrayList<String> TL = new ArrayList<String> ();
    	ArrayList<String> b = org.tools.ExcelUtil.readXml(org.tools.GetProperties.getKeyValue("ExcelPath"));
        for (int i = 0; i < b.size(); i++){
        	ArrayList<String> a = (ArrayList<String>) org.tools.ExcelUtil.readXml(org.tools.GetProperties.getKeyValue("ExcelPath")).get(i);
        	if(!TL.contains(a.get(0))){
        		TL.add(a.get(0));
        		
        	}
        }  
        
        return TL;
    }

	
}