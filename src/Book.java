
public class Book {
	
	String bname, author;
	int count=0;
	public Book(String bname, String author, int count) 
	{
		this.bname= bname;
		this.author = author;
		this.count = count;
	}
	
	public String getBname() 
	{
		return bname;
	}
	
	public String getAuthor() 
	{
		return author;
	}
	
	public int getCount()
	{
		return count;
	}
	
}
