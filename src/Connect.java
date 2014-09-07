/*
The program ask for both mysql and database credentials
Imports data from SQL table , stores them
Exports data into MongoDB

MongoDB and MySQL servers must be running
 */

import java.sql.*;
import com.mongodb.*;
import java.util.*;

public class Connect {
    private Connection con;
    private Statement st;
    private ResultSet rs;
    private DB db;
    Scanner stdin = new Scanner(System.in);

    public Connect()
    {
        //Connect to MySQL Database
        try {
            Class.forName("com.mysql.jdbc.Driver");

            System.out.println("Enter Database type (e.g mysql, SQL): ");//mysql
            String dataBaseType = stdin.nextLine();
            System.out.println("Enter Sever name (e.g localhost): ");//localhost, 127.0.0.1
            String localHost = stdin.nextLine();
            System.out.println("Enter Port number (e.g 3306): ");//3306
            String port = stdin.nextLine();
            System.out.println("Enter Database name (e.g testDB): ");//exb5209
            String dataBase = stdin.nextLine();
            System.out.println("Enter username (e.g root): ");//root, ""
            String username = stdin.nextLine();
            System.out.println("Enter password (enter 0 if no password): ");// ""
            String password = stdin.nextLine();
            if(password.equals("0")){
                password="";}


            con = DriverManager.getConnection("jdbc:"+dataBaseType+"://"+localHost+":"+port+"/"+dataBase, username, password);
            //con = DriverManager.getConnection("jdbc:mysql://localhost:3306/exb5209", "root", "");Used for quick input testing
            st = con.createStatement();
        } catch (Exception ex) {
            System.out.println("Error:" + ex);
        }
        // Connect to MongoDB
        try {
            MongoClient mongoClient = new MongoClient();
            System.out.println("Enter mongoDB Database name (e.g test): ");// ""
            String mongoDBname = stdin.nextLine();
            db = mongoClient.getDB(mongoDBname);//test
        }catch (Exception ex)
        {
            System.out.println("Error:" + ex);
        }
    }

    public void buildProjectCollection()
    {
        try {
            DBCollection coll = db.getCollection("Project");// creates Project collection
            String query = "SELECT p.Pnumber, p.Pname, d.Dname, e.Fname, e.Lname, w.Hours\n" +
                    "FROM  PROJECT p, DEPARTMENT d, WORK_ON w, EMPLOYEE e\n" +
                    "WHERE p.Dnum=d.Dnumber AND p.Pnumber=w.Pno AND w.Essn=e.Ssn\n" +
                    "ORDER BY p.Pnumber\n" +
                    "#LIMIT 5";
            rs = st.executeQuery(query);
            System.out.println(String.format("Project Collection created in DataBase %s", db.getName()));
            while (rs.next())
            {
                String currPnum = rs.getString("Pnumber");// stores the current Project number
                String nextPnum = rs.getString("Pnumber");// Stores the next Project number
                String Pnumber = rs.getString("Pnumber");
                String Pname = rs.getString("Pname");
                String Dname = rs.getString("Dname");
                BasicDBObject bsObj = new BasicDBObject("Pnumber", Pnumber).append("Pname", Pname).append("Dname", Dname); // builds project document
                List<BasicDBObject> employees = new ArrayList<BasicDBObject>(); //Stores employee documents
                while (currPnum.equals(nextPnum))
                {
                    String Fname = rs.getString("Fname");
                    String Lname = rs.getString("Lname");
                    String Hours = rs.getString("Hours");
                    BasicDBObject empObj = new BasicDBObject("Fname",Fname).append("Lname", Lname).append("Hours", Hours);//Builds employee document
                    employees.add(empObj);//Add employee document to array list
                    if (rs.next())
                    {
                        nextPnum = rs.getString("Pnumber");// Stores next Project number
                    }
                    else
                        nextPnum = null; // Signifies end of Rows
                }
                rs.previous();// undo rs.next performed to get nextPnum
                bsObj.put("Employees", employees); //embeds Employee document inside Project document
                coll.insert(bsObj);// Inserts inserts project document into project collection
            }
        }catch (Exception ex)
        {
            System.out.println("Error:" + ex);
        }
    }

    public void buildDepartmentCollection()
    {
            try
            {
                DBCollection coll = db.getCollection("Department");// creates Project collection
                String query = "SELECT d.Dname, e.Lname, dl.Dlocation\n" +
                        "FROM\tdepartment d, employee e, dept_locations dl\n" +
                        "WHERE d.Mgr_ssn=e.Ssn AND d.Dnumber=dl.Dnumber #AND e.dno=d.Dnumber\n" +
                        " ORDER BY d.Dname";
                rs = st.executeQuery(query);
                System.out.println(String.format("Department Collection created in DataBase %s", db.getName()));

                while(rs.next())
                {
                    String currPnum = rs.getString("Dname");// stores the current Department name
                    String nextPnum = rs.getString("Dname");// Stores the next Department name
                    String Dname = rs.getString("Dname");
                    String Lname = rs.getString("Lname");
                    //System.out.println(String.format("{\"Dname\": %s, \"Lname\": %s",Dname, Lname));
                    BasicDBObject bsObj = new BasicDBObject("Dname",Dname).append("Lname", Lname);//builds department document
                    List<BasicDBObject> location = new ArrayList<BasicDBObject>(); //Stores location documents

                    while(currPnum.equals(nextPnum))
                    {
                        String Dlocation = rs.getString("Dlocation");
                        //System.out.println(String.format("{\"Dlocation\": { \"Dlocation\": %s,}", Dlocation ));
                        BasicDBObject locObj = new BasicDBObject("Dlocation",Dlocation);//Builds employee document
                        location.add(locObj);//Add employee document to array list
                        if(rs.next())
                        {
                            nextPnum = rs.getString("Dname");// flag
                        }
                        else
                            nextPnum = null; // Signifies end of Rows
                    }
                    rs.previous();// undo rs.next performed to get nextPnum
                    bsObj.put("Location", location); //embeds Employee document inside Project document
                    coll.insert(bsObj);// Inserts inserts project document into project collection
                }
            }catch (Exception ex)
            {
                System.out.println("Error:" + ex);
            }
    }
}