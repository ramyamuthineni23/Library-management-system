import java.util.concurrent.TimeUnit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class WriterThread implements Runnable
{
	public static int k=0;
	private long start_time;

	public WriterThread(long start_time) {
		this.start_time=start_time;	
	}
	public void run()
	{
		try 
		{
			long end_time;
			Class.forName("com.mysql.cj.jdbc.Driver");	
			Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/library","root","");
			PreparedStatement ps = null;
			while(!ReaderThread.queue.isEmpty()||!query.poolreader.isShutdown())
			{	    
				Book book = ReaderThread.queue.take();
				if(book.getBname()==""&&book.getAuthor()==""&&book.getCount()==-1) 
				{					
					if(k==0)
					{
						 end_time = System.currentTimeMillis();
						 System.out.println("TIME TAKEN FOR THE BOOK TO BE INSERTED: "+TimeUnit.MINUTES.toMinutes(end_time-start_time));    	    		
						 k=1;
					}
					ReaderThread.queue.add(book);
					System.out.println("end time :"+System.currentTimeMillis());
					break;
				}
				ps = con.prepareStatement("INSERT INTO MAINS(BOOKNAME,AUTHORNAME,CNT) VALUES(?,?,?) ON DUPLICATE KEY UPDATE CNT=?+CNT");
		  		ps.setString(1,book.getBname());
				ps.setString (2,book.getAuthor());	 
				ps.setInt(3,book.getCount());
		  		ps.setInt(4,book.getCount());
				ps.execute();
			}			
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
