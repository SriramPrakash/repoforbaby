package com.test;

import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

public class Test2 {

	public static void main(final String... args) throws Exception {
		Connection con = null;
		ResultSet rs = null;
		Statement stmt = null;
		try {

			Class.forName("com.mysql.jdbc.Driver");
			con = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/experiment", "root", "password");
			stmt = con.createStatement();
			rs =
					stmt.executeQuery("select * from apple_pie");
			Map<String, JSONObject> jsonObjectMap = new HashMap<>();
			JSONObject jsonObject =  null;
			while (rs.next()) {
				int ruleId= rs.getInt("rule_id");
				String ruleName = rs.getString("rule_name");
				String formedKey = ruleId+ruleName;
				if(jsonObjectMap.get(formedKey)!=null){
					jsonObject = jsonObjectMap.get(formedKey);
					String problemCode = jsonObject.getString("problem_code");
					String newProblemCode = rs.getString("problem_code");
					String newproblemCodes[] = newProblemCode.split(",");
					for (String newProblemCodezz:newproblemCodes){
						if(!problemCode.contains(newProblemCodezz)){
							problemCode = problemCode+","+newProblemCodezz;
						}
					}
					jsonObject.put("problem_code",problemCode);
					String existingLocatorType = jsonObject.getString("locator_type");
					String newLocatorType = rs.getString("locator_type");
					if(!newLocatorType.equalsIgnoreCase(existingLocatorType)){
						existingLocatorType = existingLocatorType + ","+newLocatorType;
					}
					jsonObject.put("locator_type",existingLocatorType);
					JSONArray existingLocationIndicator = jsonObject.getJSONArray("locator_indicator");
					String newLocationIndicator = rs.getString("locator_indicator");
					Set<String> bugFix1 = new HashSet<>();
					for(int i=0;i<existingLocationIndicator.length();i++){
						bugFix1.add(existingLocationIndicator.getString(i));
					}
					bugFix1.add(newLocationIndicator);
					jsonObject.put("locator_indicator",bugFix1);
					//PC and instance combo
					JSONArray combo = jsonObject.getJSONArray("pc_and_inst");
					String newProblemCode1 = rs.getString("problem_code_1");
					String newInstance1 = rs.getString("no_of_inst_1");
					String newProblemCode2 = rs.getString("problem_code_2");
					String newInstance2 = rs.getString("no_of_inst_2");
					String newProblemCode3 = rs.getString("problem_code_3");
					String newInstance3 = rs.getString("no_of_inst_3");
					JSONArray singleRow = new JSONArray();
					singleRow.put(newProblemCode1+" "+newInstance1);
					singleRow.put(newProblemCode2+" "+newInstance2);
					singleRow.put(newProblemCode3+" "+newInstance3);
					combo.put(singleRow);
				}else{
					jsonObject = new JSONObject();
					String newProblemCode = rs.getString("problem_code");
					jsonObject.put("problem_code",newProblemCode);
					String newLocatorType = rs.getString("locator_type");
					jsonObject.put("locator_type",newLocatorType);
					Set<String> LocationIndicator = new HashSet<>();
					String newLocationIndicator = rs.getString("locator_indicator");
					LocationIndicator.add(newLocationIndicator);
					jsonObject.put("locator_indicator",LocationIndicator);
					JSONArray combo = new JSONArray();
					String newProblemCode1 = rs.getString("problem_code_1");
					String newInstance1 = rs.getString("no_of_inst_1");
					String newProblemCode2 = rs.getString("problem_code_2");
					String newInstance2 = rs.getString("no_of_inst_2");
					String newProblemCode3 = rs.getString("problem_code_3");
					String newInstance3 = rs.getString("no_of_inst_3");
					JSONArray singleRow = new JSONArray();
					singleRow.put(newProblemCode1+" "+newInstance1);
					singleRow.put(newProblemCode2+" "+newInstance2);
					singleRow.put(newProblemCode3+" "+newInstance3);
					combo.put(singleRow);
					jsonObject.put("pc_and_inst",combo);
					jsonObjectMap.put(formedKey,jsonObject);

				}
			}
			JSONArray finalArray = new JSONArray();
			for(String keys:jsonObjectMap.keySet()){
				JSONObject object = jsonObjectMap.get(keys);
				object.put("rule_id",keys.split("-")[0]);
				object.put("rule_name",keys.split("-")[1]);
				finalArray.put(object);
			}
			System.out.println(finalArray);
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			rs.close();
			stmt.close();
			con.close();
		}
	}

}