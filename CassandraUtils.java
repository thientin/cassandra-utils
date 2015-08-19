import com.datastax.driver.core.*;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraUtils {
	private final Logger slf4jLogger = LoggerFactory.getLogger(CassandraUtils.class);
	
	private Cluster cluster;
	private Session session;
	private String[] contactPoints;
	private String keyspaceName;
	
	public CassandraUtils(String[] contactList, String keyspaceName) {
		this.contactPoints = contactList;
		this.keyspaceName = keyspaceName;
		cluster = Cluster.builder().addContactPoints(contactPoints).build();
		session = cluster.connect(keyspaceName);
		
	}
	
	protected int AddNewKeySpace()
	{
		//TODO
		return 1;
	}
	
	protected int AddNewTable()
	{
		//TODO
		
		return 1;
	}
	
	protected int UpdateTable()
	{
		//TODO
		return 1;
	}
/*author: tin ho
 * version: 0.1
 * paras: String columnFamily, String colName, String dataType
 * return: 1 success, 2 colName duplicate, 0 exception while execute cql insert statement
 * date: 
 * */
	protected int AddColumn(String keyspaceName,String columnFamily,String colName,String dataType)
	{
		//TODO
		List columns = new ArrayList();
		
		String validate_query = String.format("select count(*) from system.schema_columns where keyspace_name='%s' and columnfamily_name='%s' and column_name='%s'",keyspaceName,columnFamily,colName );
		ResultSet results = session.execute(validate_query);
		//results.one().getInt(0);
//		for (Row row : results)
//		{
//			columns.add(row.getString("column_name"));
//		}
		if(results.one().getLong(0) != 0)
			
		{
			slf4jLogger.info(colName + " is exists");
		//	System.out.printf("%s is exists",colName);
			return 2;
		}
		
		String query = String.format("ALTER TABLE %s.%s ADD %s %s", keyspaceName,columnFamily,colName,dataType);
		
		try {
			session.execute(query);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			slf4jLogger.error(e.toString());
			return 0;
		}
	//	session.close();
		return 1;
		
	}
	/*author: tin ho
	 * version: 0.1
	 * paras: String columnFamily, String colName,
	 * return: 1 success, 2 colName primary key can't drop, 0 exception while execute cql insert statement, 3 colname doesn't exists
	 * date: 
	 * */
	
	
	
	protected int DropColumn(String keyspaceName,String columnFamily, String colName)
	{
	//TODO alter table test_tin drop emp_email;
		String validate = String.format("select type from system.schema_columns where keyspace_name='%s' and columnfamily_name='%s' and column_name='%s'",keyspaceName,columnFamily,colName );
		ResultSet results = session.execute(validate);
		Row row = results.one();
		if (row == null)
		{
			slf4jLogger.info( colName+" doesn't exists");
			return 3;
		}
		else 
		{
			if (row.toString().contains("partition_key") || row.toString().contains("clustering_key"))
			{
				slf4jLogger.info("Can't drop primary key column "+ colName);
				return 2;
			}
		}
		
		
		String query = String.format("ALTER TABLE %s.%s drop %s", keyspaceName,columnFamily,colName);
		try {
			session.execute(query);
		} catch (Exception e) {
			
			// TODO Auto-generated catch block
			e.printStackTrace();
			slf4jLogger.error(e.toString());
			System.out.printf("Can't remove column %s",colName);
			return 0;
		}
		
		return 1;
	}
	
	protected int RenameColumn(String keyspaceName,String columnFamily,String oldName, String newName)
	{
		//TODO
		String query = String.format("ALTER TABLE %s.%s rename %s to %s",keyspaceName,columnFamily,oldName,newName);
		String validate = String.format("select type from system.schema_columns where keyspace_name='%s' and columnfamily_name='%s' and column_name='%s'",keyspaceName,columnFamily,oldName );
		ResultSet results = session.execute(validate);
		Row row = results.one();
		if (row == null)
		{
			slf4jLogger.info(oldName + " doesn't exists in "+ columnFamily);
			return 2;
		}
		
		else if(row.getString("type").contains("partition_key") || row.getString("type").contains("clustering_key") )
		{
			try {
				session.execute(query);
			} catch (Exception e) {
				slf4jLogger.error(e.toString());
				return 0;
			}
			return 1; 
		}
		else 
		{
			slf4jLogger.info("Can't rename non primary column "+ oldName);
			return 4;
		}
	}
	
	protected int DropTable(String keyspaceName,String columnFamily)
	{
		String query = String.format("DROP TABLE IF EXISTS %s.%s",keyspaceName,columnFamily);
		try {
			session.execute(query);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			slf4jLogger.error(e.toString());
			return 0;
		}
		return 1;
		
	}
	
	protected int AddNewIndex()
	{
		
		//TODO
		
		return 1;
	}
	
	protected int DropIndex()
	{
		
		//TODO
		
		return 1;
		
	}
	
	public static void main(String[] args) {

		Cluster cluster;
		Session session;
		String[] contactList= {"10.128.228.244","10.128.228.246"};
		String keyspaceName = "test";
		CassandraUtils element = new CassandraUtils(contactList, keyspaceName);
	//	element.DropTable("test", "test123");
	//	element.RenameColumn("test_tin", "col0", "abc");
	//	element.AddColumn("test_tin", "email_address", "text");
	//	element.DropColumn("test_tin", "email_address");
		// Connect to the cluster and keyspace "demo"
		
	//	cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
	//	session = cluster.connect("demo");

		// Insert one record into the users table
	//	session.execute(
	//			"INSERT INTO users (lastname, age, city, email, firstname) VALUES ('Jones', 35, 'Austin', 'bob@example.com', 'Bob')");

		// Use select to get the user we just entered
	//	ResultSet results = session.execute("SELECT * FROM users WHERE lastname='Jones'");
	//	for (Row row : results) {
	//		System.out.format("%s %d\n", row.getString("firstname"), row.getInt("age"));
	//	}

		// Update the same user with a new age
	//	session.execute("update users set age = 36 where lastname = 'Jones'");

		// Select and show the change
	//	results = session.execute("select * from users where lastname='Jones'");
	//	for (Row row : results) {
	//		System.out.format("%s %d\n", row.getString("firstname"), row.getInt("age"));
	//	}

		// Delete the user from the users table
	//	session.execute("DELETE FROM users WHERE lastname = 'Jones'");

		// Show that the user is gone
	//	results = session.execute("SELECT * FROM users");
	//	for (Row row : results) {
	//		System.out.format("%s %d %s %s %s\n", row.getString("lastname"), row.getInt("age"), row.getString("city"),
	//				row.getString("email"), row.getString("firstname"));
	//	}

		// Clean up the connection by closing it
		element.cluster.close();
	}
}
