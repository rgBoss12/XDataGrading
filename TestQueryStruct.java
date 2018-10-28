package partialMarking;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Vector;

import database.DatabaseConnection;
import parsing.Node;
import parsing.QueryStructure;
import testDataGen.GenerateCVC1;
import testDataGen.preProcessForDataGeneration;

public class TestQueryStruct {
	Vector<ArrayList<Integer>> transformedQueries = new Vector<ArrayList<Integer>>();
	public TestQueryStruct(int aId,int qId,String sqlQuery) {
		try(Connection conn = new DatabaseConnection().dbConnection()){
			this.startProcessing(aId, qId, sqlQuery);
			
			System.out.println(transformedQueries);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public TestQueryStruct() {
//		Configuration config = new Configuration();
		int aId = 1;
		int qId = 1;		
		String qry = "select * from xdata_student_queries where assignment_id = ? and question_id = ?";
		try(Connection conn = new DatabaseConnection().dbConnection()){
			try(PreparedStatement pstmt = conn.prepareStatement(qry)){
				pstmt.setInt(1, aId);
				pstmt.setInt(2, qId);
//				pstmt.setString(3, rollNum);
				
				try(ResultSet rs = pstmt.executeQuery()){
					String sqlQuery = null;
					while(rs.next()){	
						sqlQuery = rs.getString("querystring");
						this.startProcessing(aId, qId, sqlQuery);
					}
					//this.initialize(aId, qId, sqlQuery);
					
		}
			
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void startProcessing(int aId,int qId,String query) {
			GenerateCVC1 cvc = new GenerateCVC1();
			
			cvc.setAssignmentId(aId);
			cvc.setQuestionId(qId);
			cvc.setQueryId(1);
			cvc.setCourseId("");

			preProcessForDataGeneration preProcess = new preProcessForDataGeneration();

			try {
				preProcess.initializeConnectionDetails(cvc);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
					
			QueryStructure qStructure=new QueryStructure(cvc.getTableMap());
					
			cvc.closeConn();

			try {
				qStructure.buildQueryStructure("1",query);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
				System.out.print("Query: ");
				
				System.out.println(query);
//			 ArrayList<Node> relation= qStructure.getLstSelectionConditions();
//			  System.out.print("Selection_Conditions:");
//			  for(int i=0;i<relation.size();i++) {
//				  System.out.print(relation.get(i).toString()+",");
//			  }
//			  System.out.println();
//				
//			  ArrayList<Node> relation1= qStructure.getLstJoinConditions();
//			  System.out.print("Join Conditions:");
//			  for(int i=0;i<relation1.size();i++) {
//				  System.out.print(relation1.get(i).toString()+",");
//			  }
//			  System.out.println();
//			  
//			  relation1= qStructure.getLstProjectedCols();
//			  System.out.print("ProjectedCols:");
//			  for(int i=0;i<relation1.size();i++) {
//				  System.out.print(relation1.get(i).toString()+",");
//			  }
//			  System.out.println();
//			  
//			  ArrayList<Node> relation2= qStructure.getLstGroupByNodes();
//			  System.out.print("Groupby Nodes:");
//			  for(int i=0;i<relation2.size();i++) {
//				  System.out.print(relation2.get(i).toString()+",");
//			  }
//			  System.out.println();
//			  System.out.println(qStructure.getWhereClauseSubqueries());
			
//			System.out.println(qStructure.getLstRelations());
				
				int noIjoin = qStructure.getNumberOfInnerJoins();
				int noOjoin = qStructure.getNumberOfOuterJoins();
				Vector<QueryStructure> fromClause = qStructure.getFromClauseSubqueries();
				Vector<QueryStructure> whereClause = qStructure.getWhereClauseSubqueries();
				ArrayList<String> setOp = qStructure.getLstSetOpetators();
				ArrayList<Node> havingCond = qStructure.getLstHavingConditions();
				
				int joinBit;
				if(noIjoin + noOjoin > 0) {
					joinBit = 1;
				}else {
					joinBit = 0;
				}
				
				int fromClauseBit = 0;
				if(fromClause.size() > 0)
				{
					fromClauseBit = 1;
				}
				
				int WhereClausebit = 0;
				if(whereClause.size() > 0)
				{
					WhereClausebit = 1;
				}
				
				int setOpBit = 0;
				if(setOp.size() > 0) {
					setOpBit = 1;
				}
				
				int havingCondBit = 0;
				
				if(havingCond.size() > 0)
					havingCondBit = 1;
				
				ArrayList<Integer> allBits = new ArrayList<Integer>(Arrays.asList(joinBit,fromClauseBit,WhereClausebit,setOpBit,havingCondBit));
				transformedQueries.addElement(allBits);
		}
}