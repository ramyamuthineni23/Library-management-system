import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
public class query
{
	public static ExecutorService poolwriter = null;
	public static ExecutorService poolreader = null;
	public void execute() throws SQLException,IOException, ClassNotFoundException
	{
		PreparedStatement ps;
		String book = null,author = null;
		ResultSet rs;
		Connection con = null;
		Class.forName("com.mysql.cj.jdbc.Driver");	
		 con=DriverManager.getConnection("jdbc:mysql://localhost:3306/library","root","");  
		Scanner scanner=new Scanner(System.in);		
		boolean run=true;
		while(run) {
			System.out.print("\n\tMenu\n1.INSERT BOOKS\n 2.DELETE BOOKS BY BOOK NAME \n3.TOTAL BOOKS AVAILABLE IN THE LIBRARY\n4.SEARCH FOR A BOOK WITH BOOKNAME\n 5.SEARCH FOR A BOOK WITH AUTHORNAME\n 6.SEARCH FOR BOOK WITH BOOKNAME AND AUTHORNAME\n7.EXIT\nENTER YOUR CHOICE : ");
			int choice1=scanner.nextInt();
			switch(choice1)
			{
			case 1:
				long start_time = System.currentTimeMillis();
				System.out.print("ENTER THE NUMBER OF FILES YOU WANT TO UPDATES :");
		   		int n=scanner.nextInt();
		   		scanner.nextLine();
		        	poolreader = Executors.newFixedThreadPool(n);   
		        	for(int i=1;i<=n;i++)
		   		{      	
		   			System.out.print("ENTER THE File "+i+" name:");
		   			String filepath = scanner.nextLine();
		       		ReaderThread reader=new ReaderThread(filepath);
		    	   		poolreader.execute(reader);
		   		}
		        	System.out.println("start time :"+System.currentTimeMillis());
		        	if(n>0)
		   		{
				  	poolwriter = Executors.newFixedThreadPool(10*n);   
				  	for(int i=1;i<=10*n;i++) 
				  	{
				  		WriterThread writer2=new WriterThread(start_time);
				  		poolwriter.execute(writer2);
				  	}
				  	poolreader.shutdown();
				  	poolwriter.shutdown();				
			   	}
		        	break;
			case 2:

				scanner.nextLine();
				String name=scanner.nextLine();
				 ps=con.prepareStatement("DELETE FROM MAINS WHERE BOOKNAME=?");
				 ps.setString (1,name);
				int deleted= ps.executeUpdate();
				if(deleted>=1)
					System.out.println("DELETED SUCCESSFULLY");
				else
					System.out.println("NO BOOK IS THERE WITH BOOKNAME");
				break;

			case 3:	
				ps=con.prepareStatement("SELECT* FROM MAINS");
    			rs=ps.executeQuery();
    			System.out.println("TOTAL BOOKS AVAILABLE IN THE LIBRARY\n");
    			while(rs.next())
    			{
    				System.out.println(rs.getString(1)+" book is written by "+rs.getString(2)+" and number of books in the library "+rs.getInt(3)+"\n");
    			}
    			break;
			case 4:
				System.out.println("ENTER BOOKNAME");
				scanner.nextLine();
				book=scanner.nextLine();
				 ps=con.prepareStatement("SELECT * FROM MAINS WHERE BOOKNAME=?");
    			ps.setString (1,book);
    			rs=ps.executeQuery();
    			rs.last();
    			if(rs.getRow()==0)
    			{
    				System.out.println("ZERO BOOKS ARE THERE FOR"+book);
    			}
    			else
    			{
    				rs.beforeFirst();
        			System.out.println("AVAILABLE BOOKS FOR The BOOK"+book+"\nBookname\tauthorname\tcount\n");
    			while(rs.next())
    			{
    				System.out.println(rs.getString(1)+"\t"+rs.getString(2)+"\t"+rs.getInt(3)+"\n");
    			}
    			}
    			break;
			case 5:
				System.out.println("ENTER AUHTORNAME");
				scanner.nextLine();
				author=scanner.nextLine();
				scanner.close();
				ps=con.prepareStatement("select * from MAINS where authorname=?");
    			ps.setString (1,book);
    			rs=ps.executeQuery();
    			rs.last();
    			if(rs.getRow()==0)
    			{
    				System.out.println("ZERO BOOKS ARE THERE FOR"+author);
    			}
    			else
    			{
    				rs.beforeFirst();
        			System.out.println("AVAILABLE BOOKS FOR The BOOK WITH "+author+"\n");
    			while(rs.next())
    			{
    				System.out.println(rs.getString(1)+"\t"+rs.getString(2)+"\t"+rs.getInt(3)+"\n");
    			}
    			}
    			break;
			case 6:
				System.out.println("ENTER BOOKNAME AND AUTHORNAME");
				book=scanner.next();	
				author=scanner.next();
				ps=con.prepareStatement("SELECT CNT FROM MAINS WHERE BOOKNAME=? and AUTHORNAME=?");
    			ps.setString (1,book);
    			ps.setString (2,author);
    			rs=ps.executeQuery();
    			System.out.println("NUMBER OF BOOKS AVAILABLE FOR "+book+"AND"+author+"IS:\n");
    			while(rs.next())
    			{
    				System.out.println(rs.getInt(1));
    			}
    			break;
			case 7:
				run=false;
    			break;
    		default:
    			System.out.println("PLEASE ENTER A VALID CHOICE BELOW");
    			break;
			}
		}
	}
	public static void main(String args[]) throws ClassNotFoundException, SQLException, IOException
	{
		query q=new query();
		q.execute();
	}
	}

