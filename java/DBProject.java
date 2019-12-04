/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */
 // git


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */
public class DBProject {

   // reference to physical database connection.
   private Connection _connection = null;

   // handling the keyboard inputs through a BufferedReader
   // This variable can be global for convenience.
   static BufferedReader in = new BufferedReader(
                                new InputStreamReader(System.in));

   /**
    * Creates a new instance of DBProject
    *
    * @param hostname the MySQL or PostgreSQL server hostname
    * @param database the name of the database
    * @param username the user name used to login to the database
    * @param password the user login password
    * @throws java.sql.SQLException when failed to make a connection.
    */
   public DBProject (String dbname, String dbport, String user, String passwd) throws SQLException {

      System.out.print("Connecting to database...");
      try{
         // constructs the connection URL
         String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
         System.out.println ("Connection URL: " + url + "\n");

         // obtain a physical connection
         this._connection = DriverManager.getConnection(url, user, passwd);
         System.out.println("Done");
      }catch (Exception e){
         System.err.println("Error - Unable to Connect to Database: " + e.getMessage() );
         System.out.println("Make sure you started postgres on this machine");
         System.exit(-1);
      }//end catch
   }//end DBProject

   /**
    * Method to execute an update SQL statement.  Update SQL instructions
    * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
    *
    * @param sql the input SQL string
    * @throws java.sql.SQLException when update failed
    */
   public void executeUpdate (String sql) throws SQLException {
      // creates a statement object
      Statement stmt = this._connection.createStatement ();

      // issues the update instruction
      stmt.executeUpdate (sql);

      // close the instruction
      stmt.close ();
   }//end executeUpdate

   /**
    * Method to execute an input query SQL instruction (i.e. SELECT).  This
    * method issues the query to the DBMS and outputs the results to
    * standard out.
    *
    * @param query the input query string
    * @return the number of rows returned
    * @throws java.sql.SQLException when failed to execute the query
    */
   public int executeQuery (String query) throws SQLException {
      // creates a statement object
      Statement stmt = this._connection.createStatement ();

      // issues the query instruction
      ResultSet rs = stmt.executeQuery (query);

      /*
       ** obtains the metadata object for the returned result set.  The metadata
       ** contains row and column info.
       */
      ResultSetMetaData rsmd = rs.getMetaData ();
      int numCol = rsmd.getColumnCount ();
      int rowCount = 0;

      // iterates through the result set and output them to standard out.
      boolean outputHeader = true;
      while (rs.next()){
	 if(outputHeader){
	    for(int i = 1; i <= numCol; i++){
		System.out.print(rsmd.getColumnName(i) + "\t");
	    }
	    System.out.println();
	    outputHeader = false;
	 }
         for (int i=1; i<=numCol; ++i)
            System.out.print (rs.getString (i) + "\t");
         System.out.println ();
         ++rowCount;
      }//end while
      stmt.close ();
      return rowCount;
   }//end executeQuery

   /**
    * Method to close the physical connection if it is open.
    */
   public void cleanup(){
      try{
         if (this._connection != null){
            this._connection.close ();
         }//end if
      }catch (SQLException e){
         // ignored.
      }//end try
   }//end cleanup

   /**
    * The main execution method
    *
    * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
    */
   public static void main (String[] args) {
      if (args.length != 3) {
         System.err.println (
            "Usage: " +
            "java [-classpath <classpath>] " +
            DBProject.class.getName () +
            " <dbname> <port> <user>");
         return;
      }//end if
      
      Greeting();
      DBProject esql = null;
      try{
         // use postgres JDBC driver.
         Class.forName ("org.postgresql.Driver").newInstance ();
         // instantiate the DBProject object and creates a physical
         // connection.
         String dbname = args[0];
         String dbport = args[1];
         String user = args[2];
         esql = new DBProject (dbname, dbport, user, "");

         boolean keepon = true;
         while(keepon) {
            // These are sample SQL statements
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. Add new customer");
				System.out.println("2. Add new room");
				System.out.println("3. Add new maintenance company");
				System.out.println("4. Add new repair");
				System.out.println("5. Add new Booking"); 
				System.out.println("6. Assign house cleaning staff to a room");
				System.out.println("7. Raise a repair request");
				System.out.println("8. Get number of available rooms");
				System.out.println("9. Get number of booked rooms");
				System.out.println("10. Get hotel bookings for a week");
				System.out.println("11. Get top k rooms with highest price for a date range");
				System.out.println("12. Get top k highest booking price for a customer");
				System.out.println("13. Get customer total cost occurred for a give date range"); 
				System.out.println("14. List the repairs made by maintenance company");
				System.out.println("15. Get top k maintenance companies based on repair count");
				System.out.println("16. Get number of repairs occurred per year for a given hotel room");
				System.out.println("17. < EXIT");

            switch (readChoice()){
				   case 1: addCustomer(esql); break;
				   case 2: addRoom(esql); break;
				   case 3: addMaintenanceCompany(esql); break;
				   case 4: addRepair(esql); break;
				   case 5: bookRoom(esql); break;
				   case 6: assignHouseCleaningToRoom(esql); break;
				   case 7: repairRequest(esql); break;
				   case 8: numberOfAvailableRooms(esql); break;
				   case 9: numberOfBookedRooms(esql); break;
				   case 10: listHotelRoomBookingsForAWeek(esql); break;
				   case 11: topKHighestRoomPriceForADateRange(esql); break;
				   case 12: topKHighestPriceBookingsForACustomer(esql); break;
				   case 13: totalCostForCustomer(esql); break;
				   case 14: listRepairsMade(esql); break;
				   case 15: topKMaintenanceCompany(esql); break;
				   case 16: numberOfRepairsForEachRoomPerYear(esql); break;
				   case 17: keepon = false; break;
				   default : System.out.println("Unrecognized choice!"); break;
            }//end switch
         }//end while
      }catch(Exception e) {
         System.err.println (e.getMessage ());
      }finally{
         // make sure to cleanup the created table and close the connection.
         try{
            if(esql != null) {
               System.out.print("Disconnecting from database...");
               esql.cleanup ();
               System.out.println("Done\n\nBye !");
            }//end if
         }catch (Exception e) {
            // ignored.
         }//end try
      }//end try
   }//end main
   
   public static void Greeting(){
      System.out.println(
         "\n\n*******************************************************\n" +
         "              User Interface      	               \n" +
         "*******************************************************\n");
   }//end Greeting

   /*
    * Reads the users choice given from the keyboard
    * @int
    **/
   public static int readChoice() {
      int input;
      // returns only if a correct value is given.
      do {
         System.out.print("Please make your choice: ");
         try { // read the integer, parse it and break.
            input = Integer.parseInt(in.readLine());
            break;
         }catch (Exception e) {
            System.out.println("Your input is invalid!");
            continue;
         }//end try
      }while (true);
      return input;
   }//end readChoice
	
	/*
    * Prints a prompt and gets user input.
    * @String
    **/
	public static String valuePrompt(String prompt) throws Exception {
		try {
			System.out.print(String.format("\t%s $", prompt));
			String input = in.readLine();
			return input;
		} catch (Exception e) {
			throw e;
		}
	}
	
	/*
	 * Gets the first column of the first row of the query
	 * @String
	 **/
	public static String getFirstElement(DBProject esql, String query) throws Exception {
		Statement stmt = esql._connection.createStatement ();
		ResultSet rs = stmt.executeQuery (query);
		ResultSetMetaData rsmd = rs.getMetaData ();
		
		// If first row does not exist...
		if(!(rs.next() && rsmd.getColumnCount() == 1)) {
			throw new Exception("Could not get first row from query. Maybe query result is empty?");
		}
		
		return rs.getString(1);
	}
	
	public static int countRowsOfTable(DBProject esql, String tableName) throws Exception {
		try {
			return Integer.parseInt(getFirstElement(esql, String.format("SELECT COUNT(*) FROM %s;", tableName)));
		} catch (Exception e) {
			throw new Exception(String.format("Could not count rows of %s", tableName));
		}
	}

			
   
   public static void addCustomer(DBProject esql)
   {
	   try 
	   {
		   String customerID = valuePrompt("Enter costumer ID:");
		   String fName = valuePrompt("Enter first name:");
		   String lName = valuePrompt("Enter last name:");  
		   String Address = valuePrompt("Enter address:");
		   String phNo = valuePrompt("Enter phone number:");
		   String DOB = valuePrompt("Enter date of birth:");  
		   String gender = valuePrompt("Enter gender type:");
		   
		   String query = String.format("INSERT INTO customer VALUES (%s, %s, '%s', %s, %s, '%s', '%s);", customerID, fName, lName, Address, phNo, DOB, gender);
		   esql.executeUpdate(query);
	   }
	   catch(Exception e)
	   {
		   System.err.println(e.getMessage());
   }


	public static void addRoom(DBProject esql){
		// Given room details add the room in the DB
		try {
			String hotelid = valuePrompt("Enter hotelid:");
			String roomno = valuePrompt("Enter roomno:");
			String roomtype = valuePrompt("Enter roomtype:");

			String query = String.format("INSERT INTO room VALUES (%s, %s, '%s');", hotelid, roomno, roomtype);
	
			esql.executeUpdate(query);
			
			System.out.println("\nSuccessfully added room.\n");
		} catch(Exception e) {
			System.err.println (e.getMessage());
		}
	}


	public static void addMaintenanceCompany(DBProject esql){
		// Given maintenance Company details add the maintenance company in the DB
		// KEVIN
		try {
			String cmpid = valuePrompt("Enter cmpid:");
			String name = valuePrompt("Enter name:");
			String address = valuePrompt("Enter address:");
			String iscertified = valuePrompt("Is this company certified? (y/n)");

			String query = String.format("INSERT INTO maintenancecompany VALUES (%s, '%s', '%s', '%s');", cmpid, name, address, iscertified);

			esql.executeUpdate(query);
			
			System.out.println("\nSuccessfully added maintenance company.\n");
		} catch(Exception e) {
			System.err.println (e.getMessage());
		}
	}

   public static void addRepair(DBProject esql){
	  // Given repair details add repair in the DB
      // Your code goes here.
      // ...
      // ...
   }//end addRepair

	public static void bookRoom(DBProject esql){
		// Given hotelID, roomNo and customer Name create a booking in the DB 
		try {
			// User inputs
			String hotelid = valuePrompt("Enter hotelid:");
			String roomno = valuePrompt("Enter roomno:");
			String customerFName = valuePrompt("Enter customer first name:");
			String customerLName = valuePrompt("Enter customer last name:");
			String bookingdate = valuePrompt("Enter booking date:");
			String noofpeople = valuePrompt("Enter number of people:");
			String price = valuePrompt("Enter price:");
			String query;
			
			
			// Calculated values
			String customerId;
			int bid;
			
			
			// Get customer id of customer
			query = String.format("SELECT customerid FROM customer WHERE fname='%s' AND lname='%s';", customerFName, customerLName);
			try {
				customerId = getFirstElement(esql, query);
			} catch(Exception e) {
				throw new Exception(String.format("Could not find customer with name %s %s", customerFName, customerLName));
			}


			// Calculate next booking id (bid)
			bid = countRowsOfTable(esql, "booking") + 1;
			
			
			// Update table
			query = String.format("INSERT INTO booking VALUES (%d, %s, %s, %s, '%s', %s, %s);", bid, customerId, hotelid, roomno, bookingdate, noofpeople, price);
			esql.executeUpdate(query);
			
			
			// Print message success
			System.out.println("\nSuccessfully added booking.\n");
			
		} catch(Exception e) {
			System.err.println (e.getMessage());
		}
	}

   public static void assignHouseCleaningToRoom(DBProject esql){
	  // Given Staff SSN, HotelID, roomNo Assign the staff to the room 
      // KEVIN
		try {
			// User inputs
			String staffssn = valuePrompt("Enter staff ssn:");
			String hotelid = valuePrompt("Enter hotel id:");
			String roomno = valuePrompt("Enter room no:");
			String query;
			
			
			// Calculated values
			int asgid;
			
			
			// Calculate next booking id (bid)
			asgid = countRowsOfTable(esql, "assigned") + 1;

			
			// Update table
			query = String.format("INSERT INTO assigned VALUES (%d, %s, %s, %s);", asgid, staffssn, hotelid, roomno);
			esql.executeUpdate(query);
			
			
			// Print message success
			System.out.println("\nSuccessfully assigned cleaning staff to room.\n");
			
		} catch(Exception e) {
			System.err.println (e.getMessage());
		}
   }
   
   public static void repairRequest(DBProject esql){
	  // Given a hotelID, Staff SSN, roomNo, repairID , date create a repair request in the DB
      // Your code goes here.
      // ...
      // ...
   }//end repairRequest
   
	public static void numberOfAvailableRooms(DBProject esql){
		// Given a hotelID, get the count of rooms available 
		// KEVIN
		try {
			// User inputs
			String hotelid = valuePrompt("Enter hotel id:");
			
			
			// Calculated values
			int roomCount;
			
		} catch (Exception e) {
			
		}
	}//end numberOfAvailableRooms
   
   public static void numberOfBookedRooms(DBProject esql){
	  // Given a hotelID, get the count of rooms booked
      // KEVIN
   }//end numberOfBookedRooms
   
   public static void listHotelRoomBookingsForAWeek(DBProject esql){
	  // Given a hotelID, date - list all the rooms available for a week(including the input date) 
      // KEVIN
   }//end listHotelRoomBookingsForAWeek
   
   public static void topKHighestRoomPriceForADateRange(DBProject esql){
	  // List Top K Rooms with the highest price for a given date range
      // Your code goes here.
      // ...
      // ...
   }//end topKHighestRoomPriceForADateRange
   
   public static void topKHighestPriceBookingsForACustomer(DBProject esql){
	  // Given a customer Name, List Top K highest booking price for a customer 
      // Your code goes here.
      // ...
      // ...
   }//end topKHighestPriceBookingsForACustomer
   
   public static void totalCostForCustomer(DBProject esql){
	  // Given a hotelID, customer Name and date range get the total cost incurred by the customer
      // Your code goes here.
      // ...
      // ...
   }//end totalCostForCustomer
   
   public static void listRepairsMade(DBProject esql){
	  // Given a Maintenance company name list all the repairs along with repairType, hotelID and roomNo
      // Your code goes here.
      // ...
      // ...
   }//end listRepairsMade
   
   public static void topKMaintenanceCompany(DBProject esql){
	  // List Top K Maintenance Company Names based on total repair count (descending order)
      // KEVIN
   }//end topKMaintenanceCompany
   
   public static void numberOfRepairsForEachRoomPerYear(DBProject esql){
	  // Given a hotelID, roomNo, get the count of repairs per year
      // Your code goes here.
      // ...
      // ...
   }//end listRepairsMade

}//end DBProject
