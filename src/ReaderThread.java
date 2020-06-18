import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
public class ReaderThread extends Thread
{
	public static int n;
	String filepath;
	public static BlockingQueue<Book> queue=new ArrayBlockingQueue<Book>(5000);	
	public ReaderThread(String filepath)
    {
    	this.filepath=filepath;
    }
	public void run()
    {
   		try {
   			
           	File file = new File(filepath);
           	if(file.exists())
           	{
       		BufferedReader brr = new BufferedReader(new FileReader(file));	
       		String line;	
       		line = brr.readLine();
       		if(line!=null)
       		{
       			Delimiter(line,brr);      				
       		}
           	}
   		}catch(FileNotFoundException e)
   		{
   			e.printStackTrace();
   		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    }
    private void Delimiter(String line, BufferedReader brr) throws ClassNotFoundException, IOException, SQLException, InterruptedException 
    {	
    	char[] delim= {'{','<','\\','>','?','.',',','_','|','{','}','[',']','(',')','+','-','"','=','|','/','!','@','#','$',';',':','%','&','^','&','*','}'};
   		char str1[] = line.toCharArray();    
   		int[] freq = new int[delim.length];  
   		//finding how many occurences
       	for(int x=0;x<delim.length;x++) 
       	{  
       		freq[x] = 0;  
       		for(int y=0;y<str1.length;y++) 
   	    	{  
       			if(delim[x]==str1[y]) 
   	    	    {  
       				freq[x]++;  	    	                      
   	            }  
   	    	} 
       	}
        int s=0;
        int large=freq[0];
        // finding the exact delimiter
        for(int x=1;x<delim.length;x++)
        {
           		if(freq[x]>large)
           		{
           			large=freq[x];
           			s=x;
           		}
           	}
           	String del = Character.toString(delim[s]);
           	position(line,del,brr);
    }
    void position(String line,String del, BufferedReader brr) throws ClassNotFoundException, IOException, SQLException, InterruptedException
    {
    	String[] l=null;
		l=line.split(del);	
		int len = l.length;
      	int bc=0,ac=0,cc=0;
		// locations of the columns
       	for(int k=0;k<len;k++)
       	{
       		if(l[k].equals("BOOKNAME"))
           	{
           		bc = k;
           	}
           	else if(l[k].equals("AUTHORNAME"))
           	{
           			ac=k;
           	}
           	else if(l[k].equals("COUNT"))
           	{
           		cc=k;
           	}
       	}
       	addQueue(bc,ac,cc,del,line,brr,len);
    }
    void addQueue(int bc,int ac,int cc,String del,String line, BufferedReader brr,int len) throws IOException, ClassNotFoundException, SQLException, InterruptedException 
    {	
    	String[] t = new String[len];
    	int max;
    	dbCreation();
    	int count=0;
    	while((line=brr.readLine())!=null)
    	{
    			t = line.split(del);
    			if((bc>=ac)&&bc>=cc)
       				max=bc;
       			else if(ac>=bc&&ac>=cc)
       				max=ac;
       			else
       				max=cc;
       			if(t.length>=max)
       			{
       				String bname = t[bc];
           			String author = t[ac];   		    		
       				if(bname!=""&&author!="")
       				{
       					if(t[cc].equals(""))
       					{
       						count=0;       	   					
       					}
       					else
       					{
       						count=Integer.parseInt(t[cc]);	       						
       					}
       					if(count>=0)
       					{
       						Book b=new Book(bname,author,count);
       						queue.put(b); 			
       					}
       				}
       			}
       	}
    	Book fb = new Book("","",-1);
    	queue.put(fb);
     }
	private void dbCreation() throws SQLException, ClassNotFoundException 
	{
		boolean flag=false;
		Class.forName("com.mysql.cj.jdbc.Driver");	
		Connection con=null;
		con= DriverManager.getConnection("jdbc:mysql://localhost:3306/library","root",""); 
		PreparedStatement ps=null;
		ps= con.prepareStatement("SELECT table_name FROM information_schema.tables where table_schema='library' ");
		ResultSet res=ps.executeQuery();
		while(res.next())
		{
			if(res.getString("TABLE_NAME").equals("MAINS"))
			{
				flag = true;
				break;
			}
		}
		if(flag==false)
		{
			ps = con.prepareStatement("CREATE TABLE MAINS(BOOKNAME VARCHAR(200),AUTHORNAME VARCHAR(200),CNT int,PRIMARY KEY(BOOKNAME,AUTHORNAME))");
			ps.executeUpdate();
			ps=con.prepareStatement("ALTER TABLE MAINS ADD INDEX (BOOKNAME,AUTHORNAME)");
			ps.executeUpdate();
		}  
	}
	    	 
  }


