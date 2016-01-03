/* Author: Siddharth Shah
 * File Description : Contains java code capable of acting as a database that can 
 * locate records based upon indexes on two (or more) fields.
 */
package db_prj2;

import java.io.*;
import java.util.*;

class person {
	public int id;
	public String fname;
	public String lname;
	public String company_name;
	public String addr;
	public String city;
	public String county;
	public String state;
	public String zip;
	public String phone1;
	public String phone2;
	public String email;
	public String web;

	public person(String record){
		String data[];

		data = record.split("\",");
		this.id = Integer.parseInt(data[0].substring(1));
		this.fname = data[1].substring(1);
		this.lname = data[2].substring(1);
		this.company_name = data[3].substring(1);
		this.addr = data[4].substring(1);
		this.city = data[5].substring(1);
		this.county = data[6].substring(1);
		this.state = data[7].substring(1);

		if(data.length == 13){
			this.zip = data[8].substring(1);
			this.phone1=data[9].substring(1);
			this.phone2=data[10].substring(1);
			this.email= data[11].substring(1);
			this.web = data[12].substring(1,data[12].length()-1);		
		}
		else {
			this.zip = data[8].substring(0,data[8].indexOf(','));
			this.phone1=data[8].substring(data[8].indexOf(',')+2);
			this.phone2=data[9].substring(1);
			this.email= data[10].substring(1);
			this.web = data[11].substring(1,data[11].length()-1);	
		}
	}
}

public class MyDatabase {
	public static TreeMap<Integer,String> id_map = new TreeMap<Integer,String>(); 
	public static TreeMap <String,String> lname_map = new TreeMap <String,String>();
	public static TreeMap <String,String> state_map = new TreeMap <String,String>();
	RandomAccessFile bfile;
	public static long position=0;

	/*
	 * Function Description : Initializes the database by creating data.db, id.ndx,state.ndx and
	 * lname.ndx files from us-500.csv file
	 * Parameter :  None
	 * Pre-condition : us-500.csv file should be saved at the given location
	 * Post-condition : None
	 */
	public void initialize_db(){
		long fsize =0;
		try{
			RandomAccessFile file = new RandomAccessFile("C:\\db_prj2\\us-500.csv", "r");
			fsize = file.length();
			file.seek(0);
			byte[] record = new byte[Integer.parseInt(String.valueOf(fsize))];
			file.read(record);
			write_bfile(new String(record));
			file.close();
		}catch(IOException e){
			e.printStackTrace();
		}	
	}

	/*
	 * Function Description : Writes records to the binary file (data.db), calls update function
	 * to update the tree maps and the indexing files
	 * Parameter :  data: All Records that needs to be written to data.db file
	 * Pre-condition : None
	 * Post-condition : None
	 */
	public  void write_bfile(String data) {
		int id=0,index=0,count=1;
		String lname=null,state=null,record=null;
		try {
			while(!data.equals("")){

				// id for i th data
				id = Integer.parseInt(data.substring(1, data.indexOf("\",")));

				//lname for ith data 
				index =0; count =1;
				// will get lname after parsing two commas
				while(count < 3){
					index = data.indexOf("\",",index+1);
					count++;
				}	

				//fname for ith data
				lname = data.substring(index+3,data.indexOf("\",",index+1));
				// We can get state info after parsing 8 commas
				count =1;index=0;
				while(count < 8){
					index = data.indexOf("\",",index+1);
					count++;
				}
				state = data.substring(index+3,data.indexOf("\",",index+1));
				record = data.substring(0,data.indexOf('$')+1);
				update_treemap(id,lname,state,position);
				bfile.writeChars(record);
				data = data.substring(data.indexOf('$')+3);
				position += (record.length()*2);
			}update_indexed_files();
		}catch(IOException e){
			e.printStackTrace();
		}
	}

	/*
	 * Function Description : Updates the tree map with the changes made to data.db files 
	 * Parameter :  id - key for id tree map
	 * 				lname - key for lname tree map
	 * 				state - key for state tree map
	 * 				address - address of record in data.db file
	 * Pre-condition : None
	 * Post-condition : None
	 */
	public void update_treemap(int id, String lname, String state, long address){
		String data = null,address_list;
		// update id treemap
		id_map.put(id,String.valueOf(address));

		//update lname treemap
		if(lname_map.containsKey(lname)){
			// lname already exists, append to the list
			address_list = lname_map.get(lname)+","+String.valueOf(address);
			lname_map.put(lname, address_list);
		}	
		else{	
			lname_map.put(lname,String.valueOf(address));
		}

		//update state treemap
		if(state_map.containsKey(state)){
			// state already exists, append to the list
			address_list = state_map.get(state)+","+String.valueOf(address);
			state_map.put(state, address_list);
		}	
		else
			state_map.put(state,String.valueOf(address));
	}

	/*
	 * Function Description : Updates the indexing files by copying all the tree map data to 
	 * indexing files   
	 * Parameter :  None
	 * Pre-condition : None
	 * Post-condition : None
	 */
	public void update_indexed_files(){
		String address;
		String data =null,address_list;

		//Update id indexing file
		try {
			RandomAccessFile id_file = new RandomAccessFile("C:\\db_prj2\\id.ndx", "rw");
			id_file.setLength(0);
			id_file.seek(0);
			for(Integer id : id_map.keySet()){
				address = id_map.get(id);
				// get id and address for each record
				data = id+" -> "+address+"\n";
				// save the id, address pair to id.ndx file
				id_file.write(data.getBytes());
				data = null;
			}
			id_file.close();

			//update lname indexing file
			RandomAccessFile lname_file = new RandomAccessFile("C:\\db_prj2\\lname.ndx", "rw");
			lname_file.setLength(0);
			lname_file.seek(0);
			for(String lname : lname_map.keySet()){
				data = null;
				address_list = lname_map.get(lname);
				// get lname and address for each record
				data = lname+" -> "+address_list+"\n";
				// save the lname, address pair to lname.ndx file
				lname_file.write(data.getBytes());		
			}
			lname_file.close();

			//update state indexing file
			RandomAccessFile state_file = new RandomAccessFile("C:\\db_prj2\\state.ndx", "rw");
			state_file.setLength(0);
			state_file.seek(0);
			for(String state : state_map.keySet()){
				data = null;
				address_list = state_map.get(state);
				// get state and address for each record
				data = state+" -> "+address_list+"\n";
				// save the state, address pair to state.ndx file
				state_file.write(data.getBytes());
			}
			state_file.close();
		}catch(IOException e){
			e.printStackTrace();
		}	
	}

	/*
	 * Function Description : Reads record from binary file (data.db) and returns the record 
	 * as an object of person class
	 * Parameter : pos - address of the record in data.db file  
	 * Pre-condition : None
	 * Post-condition : None
	 */
	public person read_bfile(int pos) {
		person p_info=null;
		String data[],id=null;

		try {
			bfile.seek(pos);
			String record = "";
			char c = ' ';
			// read the record till you encounter end of record symbol('$')
			while(c != '$'){
				c = bfile.readChar();
				if(c!='$')
					record += c;
			}
			// gather all record info into different fields of personal data
			p_info = new person(record);
		}catch(IOException e){
			e.printStackTrace();
		}
		return p_info;
	}

	/*
	 * Function Description : Prints the record 
	 * Parameter :  p_info - record containing all the information of a person
	 * Pre-condition : None
	 * Post-condition : None
	 */
	public void print_record(person p_info){		
		System.out.println("-----------------Personal Info-------------------");
		System.out.println("id : "+p_info.id);
		System.out.println("first_name: "+p_info.fname);
		System.out.println("last_name: "+p_info.lname);
		System.out.println("company_name: "+p_info.company_name);
		System.out.println("address: "+p_info.addr);
		System.out.println("city: "+p_info.city);
		System.out.println("county: "+p_info.county);
		System.out.println("state: "+p_info.state);
		System.out.println("zip: "+p_info.zip);
		System.out.println("phone1: "+p_info.phone1);
		System.out.println("phone2: "+p_info.phone2);
		System.out.println("email: "+p_info.email);
		System.out.println("web: "+p_info.web);
		System.out.println("-------------------------------------------------");
	}

	/*
	 * Function Description : Gets the address of the record from the indexed files 
	 * Parameter :  key - used for looking up the indexed files
	 * 				type - indicates if the key is a id/lname/state
	 * Pre-condition : None
	 * Post-condition : None
	 */
	public String get_address(String key, int type){
		String address=null;
		if(type ==1){
			if(id_map.containsKey(Integer.parseInt(key)))
				address = id_map.get(Integer.parseInt(key));
			else
				terminate(key);	
		}	
		else if(type ==2){
			if(lname_map.containsKey(key))
				address = lname_map.get(key);
			else
				terminate(key);
		}
		else if(type ==3){
			if(state_map.containsKey(key))
				address = state_map.get(key);
			else
				terminate(key);
		}
		else{
			System.out.println("Error!! wrong type value");
			terminate(key);
		}	
		return address;
	}

	/*
	 * Function Description : Terminates the query by displaying the message 
	 * Parameter :  key - index value, using which the indexed file is to be queried
	 * Pre-condition : None
	 * Post-condition : None
	 */
	public void terminate(String key){
		System.out.println("Error!! "+key+" does not exist");
		System.out.println("==========================================================");
	}

	/*
	 * Function Description : Selects and displays the desired record from the data.db file 
	 * Parameter :  key - index value, using which the record is selected from data.db file
	 * 				type - indicates if the key is a id/lname/state
	 * Pre-condition : None
	 * Post-condition : None
	 */
	public void select(String key, int type) {
		int i=0;
		String address=null, address_list[];
		person p_info = null;
		address = get_address(key, type);
		if(address == null)
			return;
		address_list = address.split(",");
		System.out.println("Output for select("+key+" , "+type+")");
		if(address_list.length == 0){
			p_info = read_bfile(Integer.parseInt(address));
			print_record(p_info);
		}
		else {
			while(i < address_list.length){
				p_info = read_bfile(Integer.parseInt(address_list[i]));
				print_record(p_info);
				i++;
			}	
		}
		System.out.println("==========================================================");
	}

	/*
	 * Function Description : Inserts the given record to data.db file and updates the 
	 * indexing files
	 * Parameter :  record - contains the data that needs to be inserted to data.db file
	 * Pre-condition : None
	 * Post-condition : None
	 */
	public void insert(String record){
		int id;
		String p_info[];
		System.out.println("Output for Insert("+record+")");
		record = record+"$"+" "+" ";
		p_info = record.split("\",");
		id = Integer.parseInt(p_info[0].substring(1));
		// if id already exists, then send an error message and exit
		if(id_map.containsKey(id)){
			System.out.println("Error!!! Id already exists");
			System.out.println("==========================================================");
			return;
		}
		write_bfile(record);
		System.out.println("Record Inserted successfully !!");
		System.out.println("==========================================================");

	}

	/*
	 * Function Description : Deletes the records corresponding to the key
	 * Parameter : key - index value, using which the record is selected from data.db file
	 * 			   type - indicates if the key is a id/lname/state  
	 * Pre-condition : 
	 * Post-condition : 
	 */
	public void delete(String key, int type){
		int i=0;
		String address=null, address_list[];
		System.out.println("Output for Delete("+key+" , "+type+")");
		address = get_address(key,type);
		if(address == null)
			return;
		address_list = address.split(",");
		if(address_list.length == 0){
			delete_record_from_index_files(Integer.parseInt(address), type);
			delete_record_from_bfile(Integer.parseInt(address));
		}
		else {
			while(i < address_list.length){
				delete_record_from_index_files(Integer.parseInt(address_list[i]), type);
				delete_record_from_bfile(Integer.parseInt(address_list[i]));
				i++;
			}	
		}
		update_indexed_files();
		System.out.println("Record containing "+key+" is deleted Successfully !!");
		System.out.println("==========================================================");	
	}

	/*
	 * Function Description : Deletes the record from the binary file(data.db) 
	 * Parameter :  pos - address of the record in data.db file
	 * Pre-condition : None
	 * Post-condition : None
	 */
	public void delete_record_from_bfile(int pos){
		char c=' ';
		int record_size=0;
		String modified_record = "0", record="";

		try {
			bfile.seek(pos);
			while(c != '$'){
				c = bfile.readChar();
				if(c!='$'){
					// saving record, used while removing all its enteries from the tree maps
					record+=c;
					// used while marking the record as zeroes
					record_size++;
				}	
			}

			//deleting from data.db file - overwriting record with zeroes
			bfile.seek(pos);
			while(record_size > 0){
				modified_record += "0";
				record_size --;
			}
			bfile.writeChars(modified_record);
		}catch(IOException e){
			e.printStackTrace();
		}
	}

	/*
	 * Function Description : Deletes the record from the indexing files 
	 * Parameter :  pos - address of the record in data.db file
	 * 				type - indicates if the key is a id/lname/state  
	 * Pre-condition : None
	 * Post-condition : None
	 */
	public void delete_record_from_index_files(int pos, int type){
		int id;
		String last_name=null, state=null, address=null, new_address=null;
		person p_info=null;

		p_info = read_bfile(pos);
		id = p_info.id;
		last_name = p_info.lname;
		state = p_info.state;

		// Step1: always remove from the id tree map
		id_map.remove(id);

		// Step 2: find if there are one or addresses in the lname.ndx
		address = lname_map.get(last_name);
		new_address = delete_address_from_address_list(address, pos);
		// if multiple address then save the modified address back  
		lname_map.remove(last_name);
		if(new_address != null)
			lname_map.put(last_name, new_address);
		
		//Step 3: find if there are one or addresses in the state.ndx
		address = state_map.get(state);
		new_address = delete_address_from_address_list(address, pos); 
		// if multiple address then save the modified address back 
		state_map.remove(state);
		if(new_address != null)
			state_map.put(state, new_address);
		update_indexed_files();
	}

	/*
	 * Function Description : Deletes the address from the given address list
	 * Parameter :  address - can be a collection addresses seperated by "," or just a
	 * 						  single address
	 * 				pos - address to be deleted from the address list set
	 * Pre-condition : None
	 * Post-condition : None
	 */
	public String delete_address_from_address_list(String address, int pos){
		String new_address="", address_list[];
		int len = 0;
        if(address == null)
        	return null;
		address_list = address.split(",");
		if(address_list.length > 1){
			for(String s: address_list){
				if(Integer.parseInt(s) == pos)
					s = " ";
				else 
					new_address = new_address+s+",";
			}
			len = new_address.length(); 
			return (new_address.substring(0,len-2));
		}
		else 
			return null;
	}

	/*
	 * Function Description : Modifies the record
	 * Parameter :  key - index value, used while searching the address of the record in 
	 * 					  indexing files
	 * 				field_type - record field no which needs to be modified where 1 - id, 
	 * 							2 - first_name, 3 - last_name, 4 - company_name, 5 - address,
	 * 							6 - city,7 - county, 8 - state, 9 - zip, 10 - phone1, 
	 * 							11 - phone2, 12 - email, 13 - web
	 * 				modified_data - new data that needs to be updated into the record
	 * Pre-condition : None
	 * Post-condition : None
	 */
	public void modify(int key,int field_type, String modified_data){
		String address= null,record=null, new_address=null;
		person p_info = null, new_p_info;

		System.out.println("Output for modify("+key+" , "+field_type+" , "+modified_data+")");
		System.out.println("Before Modification :");
		
		// step 1: find the original record from data.db file
		address = get_address(String.valueOf(key), 1);
		if(address == null)
			return;

		//  Got original record from data.db
		p_info = read_bfile(Integer.parseInt(address));
		print_record(p_info);
		//step 2: alter the required field
		new_p_info = modify_personal_info(field_type, p_info, modified_data);
		if(new_p_info == null)
			return;

		// step 3: club all the fields and right back to the same location of data.db file
		record = "\""+new_p_info.id+"\",\""+new_p_info.fname+"\",\""+new_p_info.lname+"\","
				+ "\""+new_p_info.company_name+"\",\""+new_p_info.addr+"\",,\""+new_p_info.city+"\""
				+ ",\""+new_p_info.county+"\",\""+new_p_info.state+"\",\""+new_p_info.zip+"\","
				+ "\""+new_p_info.phone1+"\",\""+new_p_info.phone2+"\",\""+new_p_info.email+"\","
				+ "\""+new_p_info.web+"\"$"+" "+" ";

		//step 4: write the modified record to data.db file
		try {
		bfile.seek(Integer.parseInt(address));		
		bfile.writeChars(record);
		}catch(IOException e){
			e.printStackTrace();
		}
		// step 5: if the field to be modified is id, last_name or state 
		// then modify the hash maps also 
		if(field_type == 1){
			// step 5a :this id is to be deleted and new id is to be added to the id_hashmap
			id_map.remove(p_info.id);
			id_map.put(new_p_info.id, address);
		}
		else if(field_type == 2){
			// step  5b: handle first the old last name and address mapping
			new_address = delete_address_from_address_list(lname_map.get(p_info.lname), 
					Integer.parseInt(address));
			//delete the old address
			lname_map.remove(p_info.lname);
		   //add the new address after removing the address from its address list
			if(new_address != null)
				lname_map.put(p_info.lname, new_address);
			// step 5c: now add the new last name and address mapping 
			lname_map.put(new_p_info.lname, address);
		}
		else if(field_type == 3){
			// step  5d: handle first the old state and address mapping
			new_address = delete_address_from_address_list(state_map.get(p_info.state), 
					Integer.parseInt(address));
			//delete the old address
			state_map.remove(p_info.state);
		   //add the new address after removing the address from its address list
			if(new_address != null)
				state_map.put(p_info.state, new_address);
			// step 5e: now add the new state and address mapping 
			state_map.put(new_p_info.state, address);
		}
		else {
			//its not an id, state or lastname field updation, so do nothing 
		}
		// step 5f : finally update the changes to indexed files
		update_indexed_files();
		System.out.println("Record with key : "+key+" is modified successfully");
		System.out.println("After Modification : ");
		print_record(new_p_info);
		System.out.println("==========================================================");	
	}
	
	/*
	 * Function Description : Updates the desired field of the record
	 * Parameter :  field_type - record field no which needs to be modified where 1 - id, 
	 * 							2 - first_name, 3 - last_name, 4 - company_name, 5 - address,
	 * 							6 - city,7 - county, 8 - state, 9 - zip, 10 - phone1, 
	 * 							11 - phone2, 12 - email, 13 - web
	 * 				p_info - contains the record 
	 * 				modified_data - new data that needs to be updated into the record
	 * Pre-condition : 
	 * Post-condition : 
	 */
	public person modify_personal_info(int field_type, person p_info, String modified_data){
		switch(field_type){
		case 1:
			p_info.id = Integer.parseInt(modified_data);
			break;
		case 2:
			p_info.fname = modified_data;
			break;
		case 3:
			p_info.lname = modified_data;
			break;
		case 4:
			p_info.company_name = modified_data;
			break;
		case 5:
			p_info.addr = modified_data;
			break;
		case 6:
			p_info.city = modified_data;
			break;
		case 7:
			p_info.county = modified_data;
			break;
		case 8:
			p_info.state = modified_data;
			break;
		case 9:
			p_info.zip = modified_data;
			break;
		case 10:
			p_info.phone1 = modified_data;
			break;
		case 11:
			p_info.phone2 = modified_data;
			break;
		case 12:
			p_info.email = modified_data;
			break;
		case 13:
			p_info.web = modified_data;
			break;
		default :
			System.out.println("invalid field type");
			return null;
		}
		return p_info;
	}

	/*
	 * Function Description : Counts the no of records in the database by counting the entries in
	 * index files
	 * Parameter :  key - index value, used while searching the address of the record in 
	 * 					  indexing files
	 * 				type - indicates if the key is a id/lname/state
	 * Pre-condition : None
	 * Post-condition : None
	 */
	public void count(String key, int type){
		int no_of_records=0;
		String address=null, address_list[];

		if(type == 1)
			no_of_records = id_map.size();
		else {
			address = get_address(key, type); 
			if(address == null)
				return;
			// single record 
			if(address.indexOf(',') == 0)
				no_of_records = 1;
			else{
				address_list = address.split(",");
				no_of_records = address_list.length;
			}	
		}	
		System.out.println("Output for count("+key+", "+type+") is :"+no_of_records);
		System.out.println("==========================================================");	
	}


	/*
	 * Function Description : Opens the data.db file and calls select, insert, delete,
	 * modify, and count modules to test the database functionalities.
	 * Parameter :  None
	 * Pre-condition : None
	 * Post-condition : None
	 */

	public static void main(String args[]){
		String record;
		MyDatabase mydb = new MyDatabase();

		// Opening data.db file in read - write mode
		try{
			mydb.bfile = new RandomAccessFile("C:\\db_prj2\\data.db", "rw");
			mydb.bfile.setLength(0);
			mydb.initialize_db();


			//Demo of different types of commands
			//Note : second parameter in each field is the indexing file type 
			//1 for id, 2 for last_name, 3 for state
			//1. select command - selection based on id/last_name/state
			System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
			mydb.select("405",1);
			mydb.select("Abdallah",2);
			mydb.select("TX",3);

			//2. insert command - insert into data.db file and update all 3 indexed files
			System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
			record = "\"504\",\"Twinkle\",\"Gupta\",\"XYZ\",\"7740 McCallum Blvd\",\"Dallas\","
					+"\"Dallas\",\"TX\",\"75252\",\"469-309-1888\",\"504-845-1429\",\"twinkle.0801@gmail.com\","
					+"\"http://www.twinklegupta.com\"";
			mydb.insert(record);
			// inserting a already existent record id
			record = null;
			record = "\"460\",\"James\",\"Butt\",\"Benton, John B Jr\",\"6649 N Blue Gum St\",\"New Orleans\","
					+"\"Orleans\",\"LA\",\"70116\",\"504-621-8927\",\"504-845-1427\",\"jbutt@gmail.com\","
					+"\"http://www.bentonjohnbjr.com\"";
			mydb.insert(record);

			//3. delete command - deletion based on id/last_name/state
			System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
			mydb.delete("256",1);
			mydb.delete("Chavous",2);
			mydb.delete("IN",3);
			//deleting a non-existent last name
			mydb.delete("Jaiswal",2);

			//4. modify command - modify based on id/last_name/state
			// third parameter in the modify function is the parameter which is to be modified
			// 1 - id, 2 - first_name, 3 - last_name, 4 - company_name, 5 - address, 6 - city,
			// 7 - county, 8 - state, 9 - zip, 10 - phone1, 11 - phone2, 12 - email, 13 - web
			System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
			mydb.modify(378,4, "Texas Caster & Abcde Co");
			mydb.modify(410,7, "Dallas");
			mydb.modify(267,8,"TX");

			//5. count command - count no of database enteries based on id/last_name/state
			System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
			mydb.count("0",1);//count overall no of records in the database
			mydb.count("Witten", 2);// count all records in which last_name is Witten
			mydb.count("IL", 3); // count all records in which state is IL
			System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
			mydb.bfile.close();
		}catch(IOException e){
			e.printStackTrace();
		}
	}
}

