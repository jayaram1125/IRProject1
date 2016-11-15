

package project2IR;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.FileSystems ;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Set;
import java.util.LinkedList ;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;




public class IndexRead 
{
	
	//Hash map to store inverted index 
    private HashMap<String, LinkedList<Integer>> invertedindex ;  
    
    //Variables used to store comparisons for each algorithm 
    private int taatandcount ;
	private int taatorcount ;
	private int daatandcount ;
	private int daatorcount ;
	
	//Variables for the files used 

	
	public File inputfile ;  
	public File outputfile ;

	private LinkedList<LinkedList<Integer>> daatlist;
	private OutputStreamWriter outwriter;
	
	
	/*
	 * Method: IndexRead
	 * Description : Constructor   
	 * 
	 */
	
	public IndexRead()  throws IOException
	{
	
		invertedindex =   new HashMap<String, LinkedList<Integer>>();
		taatandcount = 0;
	    taatorcount = 0;
		daatandcount = 0;
		daatorcount = 0 ;
		
	    daatlist = new LinkedList<LinkedList<Integer>>(); 
		
	}
	
	
	/*
	 * Method: GetInputAndProcess
	 * Description : Method used to read input file and process the further steps    
	 * 
	 */
	
	public void GetInputAndProcess() throws IOException
	{
	
		BufferedReader breader = new BufferedReader(new InputStreamReader(new FileInputStream(inputfile), "UTF8"));
		String linestring = new String();
		
		outwriter = new OutputStreamWriter(new FileOutputStream(outputfile,true), "UTF-8");
		
		while ((linestring = breader.readLine()) != null)
		{
			String[] terms = linestring.split(" ");
			
			GetPostings(terms);
			
			//Reset the counts for every input operation 
			taatandcount = 0;
		    taatorcount = 0;
			daatandcount = 0;
			daatorcount = 0 ;
		    
			TAATAndQuery(terms) ;
			TAATOrQuery(terms) ;
			
			DAATUtil(terms); 
			DAATAndQuery(terms) ;
		    DAATOrQuery(terms) ;
		    daatlist.clear();
			
		}
		
		outwriter.close();
		breader.close();
	}
	
	
	
	public static void main(String[] args) throws IOException
	{
		
		IndexRead myindexread  =  new IndexRead(); 
		
		myindexread.inputfile = new File(args[2]); 		
		//myindexread.inputfile = new File("C:/Users/JayaramK/Desktop/input.txt"); 
		
		myindexread.outputfile = new File (args[1]);
		//myindexread.outputfile = new File("C:/Users/JayaramK/Desktop/output.txt"); 
		
		myindexread.GenerateInvertedIndex(args[0]);
		
		//myindexread.GenerateInvertedIndex("C:/Users/JayaramK/Desktop/index");
		myindexread.GetInputAndProcess();
		
	 }
	
	
	/*
	 * Method: GenerateInvertedIndex
	 * Description : Method used to build inverted index for all the terms extracted    
	 * 
	 */
	public void  GenerateInvertedIndex (String path) throws IOException
	{
		
		 FileSystem fs = FileSystems.getDefault();
		 Path path1 = fs.getPath(path);
		 IndexReader reader = null ;
	     try
   	     {	 
			 reader = DirectoryReader.open(FSDirectory.open(path1));			 
		 }
		 catch(IOException e2)
		 {
			 System.out.println("Index File not found");
		 }
	     
	     
	 
	     Terms[] myterms = new Terms [12];
	 
	   
	     //Retrieving Terms for each indexed field 
	     myterms[0] = MultiFields.getTerms(reader,"text_nl");
	     myterms[1] = MultiFields.getTerms(reader,"text_fr");
	     myterms[2] = MultiFields.getTerms(reader,"text_de");
	     myterms[3] = MultiFields.getTerms(reader,"text_ja");
	     myterms[4] = MultiFields.getTerms(reader,"text_ru");
	     myterms[5] = MultiFields.getTerms(reader,"text_pt");
	     myterms[6] = MultiFields.getTerms(reader,"text_es");
	     myterms[7] = MultiFields.getTerms(reader,"text_es");
	     myterms[8] = MultiFields.getTerms(reader,"text_it");
	     myterms[9] = MultiFields.getTerms(reader,"text_da");
	     myterms[10] = MultiFields.getTerms(reader,"text_no");
	     myterms[11] = MultiFields.getTerms(reader,"text_sv");
	     
	     
	    
	    //generating the inverted index
	    for (int i= 0 ; i<12 ;++i)
	    {	 
	    	 TermsEnum tempterm = myterms[i].iterator();
		     while(tempterm.next() != null)
		     {
		    	 	BytesRef term = tempterm.term() ;
		    	    String TermString = term.utf8ToString();
		    	    PostingsEnum postenum = tempterm.postings(null, PostingsEnum.NONE);
		    	    LinkedList<Integer>docidlist = new LinkedList<Integer>() ;
		    	    while(postenum.nextDoc() != PostingsEnum.NO_MORE_DOCS)
		    	    { 
		    	    	int temp = postenum.docID();
		    	    	docidlist.add(temp);	
		    	    }
		    	    invertedindex.put(TermString,docidlist) ;	 
		    	   
		    	    
		     }
		     
	     	     
	      }
	          
	 }
	
	/*
	 * Method: GetPostings
	 * Description : Method used to print posting list for each term   
	 * 
	 */
		

	public void GetPostings(String[] qterms) throws IOException
	{
	
		
		for(String qterm : qterms)
		{
			 
		     outwriter.write("GetPostings") ;
		     outwriter.write(System.getProperty("line.separator"));
		     outwriter.write(qterm);
		     outwriter.write(System.getProperty("line.separator"));
		     outwriter.write("Postings list:");
			 
			 LinkedList<Integer> arr =  invertedindex.get(qterm);
			 for(int i = 0 ; i<arr.size();++i)
			 {			 
				 outwriter.write(" "+arr.get(i)) ; 
			 }
	
			 outwriter.write(System.getProperty("line.separator"));
			 outwriter.flush();
		}
		
		
		
	}	
	
	/*
	 * Method: PrintUtil
	 * Description : Method used for printing the results   
	 * 
	 */
	
	public void PrintUtil(LinkedList<Integer>finallist,int count) throws IOException 
	{
		
		Set<Integer> templist = new HashSet<Integer>();
		templist.addAll(finallist);
		finallist.clear();
		finallist.addAll(templist);
		
		Collections.sort(finallist);

		outwriter.flush();
		
		if(finallist.isEmpty())
		{
			outwriter.write(" empty");
		}
		else
		{
			for(int i = 0 ;i < finallist.size();++i)
			{
				outwriter.write(" "+finallist.get(i));
			} 
		}
		
		
		outwriter.write(System.getProperty("line.separator"));
		outwriter.write("Number of documents in results: ") ;
		
		
		outwriter.write(finallist.size()+"") ;
		
	
		outwriter.write(System.getProperty("line.separator"));
		outwriter.write("Number of comparisons: ") ;
		outwriter.write(count+"") ;
		
		
		outwriter.write(System.getProperty("line.separator"));
		outwriter.flush();
	}
	
	
	/*
	 * Method: TAATAndQueryUtil
	 * Description : Method used by TAATAndQuery method  
	 * 
	 */
	
	public LinkedList<Integer> TAATAndQueryUtil(LinkedList<Integer>list1,LinkedList<Integer>list2)
	{
		
		LinkedList<Integer>list3 = new LinkedList<Integer>();
		int i = 0 ;
		int j=  0 ;
		while(i<list1.size()&& j<list2.size())
		{
			int val1 = list1.get(i);
			int val2 = list2.get(j);
			
			
			if(val1 == val2)
			{
				list3.add(val1);
				i++;
				j++;
				taatandcount ++;
				
			}
			else if(val1 <val2)
			{
				i++;
				taatandcount ++;
			}
			else
			{
				j++ ;
				taatandcount ++;
			}
						
		}
		
		return list3;
	}
	
	/*
	 * Method: TAATAndQuery
	 * Description : Method used to implement TAATAndQuery algorithm 
	 * 
	 */
	
	public void TAATAndQuery(String[] qterms) throws IOException
	{
		
		outwriter.write("TaatAnd") ;
		outwriter.write(System.getProperty("line.separator"));
		
		for(String qterm : qterms)
		{
			outwriter.write(qterm+" ") ;
			
		}
		outwriter.write(System.getProperty("line.separator"));
		outwriter.write("Results:") ;
		
		
		int size = qterms.length;
		
		
		LinkedList<Integer>list1 = invertedindex.get(qterms[0]); 
		LinkedList<Integer>list2 = invertedindex.get(qterms[1]);
		
		//For the first two lists the TAATAndquery is run 
		LinkedList<Integer>finallist = TAATAndQueryUtil(list1,list2);
		
		for(int i = 2 ;i<size ;++i)
		{
			//The result of previous operation and the current linked list are used as input
			finallist = TAATAndQueryUtil(finallist,invertedindex.get(qterms[i]));
			
		}
		
		outwriter.flush();
		PrintUtil(finallist,taatandcount);
	}
	
	
	/*
	 * Method: TAATOrQueryUtil
	 * Description : utitlity function used by  TAATOrQuery method 
	 * 
	 */
	public LinkedList<Integer> TAATOrQueryUtil(LinkedList<Integer>list1,LinkedList<Integer>list2)
	{
		
		LinkedList<Integer>list3 = new LinkedList<Integer>();
		int i = 0 ;
		int j=  0 ;
		while(i<list1.size()&& j<list2.size())
		{
			int val1 = list1.get(i);
			int val2 = list2.get(j);
			if(val1 < val2)
			{
				list3.add(val1);
				i++;				
				taatorcount ++;
			}
			else if(val1 == val2)
			{
				list3.add(val1);
				i++;
				j++;
				taatorcount ++;				
			}
			else 
			{
				list3.add(val2);
				j++;
				taatorcount ++;
			}
					
		}

		while(i<list1.size())
		{
			list3.add(list1.get(i));
			i++;
		}

		while(j<list2.size())
		{
			list3.add(list2.get(j));
			j++;
		}
		return list3;
	}
	
	
	
	/*
	 * Method: TAATOrQuery
	 * Description : It implements the DAAT OR algorithm 
	 * 
	 */
	
	
	public void TAATOrQuery(String[] qterms) throws IOException
	{
		
		outwriter.write("TaatOr") ;
		outwriter.write(System.getProperty("line.separator"));
		
		for(String qterm : qterms)
		{
			outwriter.write(qterm+" ") ;
			
		}
		outwriter.write(System.getProperty("line.separator"));
		outwriter.write("Results:") ;
		
		int size = qterms.length;
		
		
		LinkedList<Integer>list1 = invertedindex.get(qterms[0]); 
		LinkedList<Integer>list2 = invertedindex.get(qterms[1]);
	
		//For the first  two linked lists Taator is run 
		LinkedList<Integer>finallist = TAATOrQueryUtil(list1,list2);
		
		for(int i = 2 ;i<size ;++i)
		{
			//The result of previous operation and the current linked list are used as input
			finallist = TAATOrQueryUtil(finallist,invertedindex.get(qterms[i]));
			
		}
		outwriter.flush();
		PrintUtil(finallist,taatorcount);
		
	}
	
	
	public void  DAATUtil(String[] qterms ) 
	{
	 
	    for(int i = 0 ;i <qterms.length ;++i)
	    {
	         LinkedList<Integer> docidlist =  invertedindex.get(qterms[i]);         
	         daatlist.add(docidlist);	
	         
	    }
		
	}
	
	/*
	 * Method: DAATAndQuery
	 * Description : It implements the DAAT AND algorithm 
	 * 
	 */
	public void DAATAndQuery(String[] qterms) throws IOException
	{
		
		
		//System.out.println("DAATAndQuery");
		outwriter.write("DaatAnd") ;
		outwriter.write(System.getProperty("line.separator"));
	
		for(String qterm : qterms)
		{
			outwriter.write(qterm+" ") ;
			
		}
		outwriter.write(System.getProperty("line.separator"));
		outwriter.write("Results:") ;
		
		
		//Linked List for storing the final  DAAT results 
		LinkedList<Integer>finallist = new LinkedList<Integer>();
		
		int [] pl = new int[qterms.length];
		
		
		//Pointers for each linked list are stored in an array .Initializing them 
		for(int i = 0 ;i<qterms.length ;++i)
		{
			pl[i] = 0 ;
			
		}
		
		//Elements for each iteration for comparison 
		int [] el = new int[qterms.length];
		for(int i = 0 ;i<qterms.length ;++i)
		{
			el[i] = 0 ;
			
		}
				
		//Storing length of each linked list in an array 
		int[] listlengths = new int[qterms.length];
		for(int i= 0 ; i<qterms.length;++i)
		{
			listlengths[i] = daatlist.get(i).size();
			
		}
		
		int maxlimit  = 0 ;
		
		//Checking whether we have reached out of bound of every linked list 
		//Maxlimit is the number of linked lists which are out of bound 
		while(maxlimit < qterms.length) 
		{
			
			//System.out.println("while main");

		
			boolean bvalue = false ;
			int val = 0 ;
			
		
		
			int comp = 0 ;
			int comppos = 0 ; 
		
			//Maximum element has to be picked up after every iteration of comparison across all the linked lists 
			for(int row = 0 ; row <qterms.length ;++row)
			{
			
				if(pl[row]<listlengths[row])
				{
					
					if(daatlist.get(row).get(pl[row]) > comp)
					{
						comp = daatlist.get(row).get(pl[row]);
						comppos = row;
					}
				}
				
			}
			
			//System.out.println("comppos= "+comppos);
	    	//System.out.println("comp= "+comp);
		
			for(int row = 0 ; row < qterms.length; ++row)
			{
			
			
				while(pl[row] < listlengths[row])
				{
					
					//System.out.println("row = "+row+" ,pl[row] ="+pl[row]);
					
					int temp = daatlist.get(row).get(pl[row]);
				    //System.out.println("temp = "+temp);
					
					
					    
					if(temp == comp )
					{
						if(comppos != row)
						{
							daatandcount++;
						}
						el[row] = temp ;
						pl[row]++;
						break;
					}

					else if(temp < comp)
					{
						
						pl[row]++;
						daatandcount++;
						
					}
					else if(temp>comp)
					{
						
						daatandcount++;
						break;
					}
					//char x =  (char) System.in.read();
					
					
			   }
				
			}
			
			//Iterating across each term to check whether there is a same docid at a position in each row 
			for(int row = 1 ; row < qterms.length; ++row)
			{
			
				
										 
					 val = el[0] ;
					// System.out.println("------------"+el[row]);
					 if( val == el[row])
					 {
						 bvalue  = true ;  
						// System.out.println("------------");
						
					 }
					 else
					 {  
						 bvalue  = false ;
						 break;
					 }
				
				
			
			}
			
			// if there is a similar docid value in each term row,it has to be added to posting list
			if(bvalue == true)
			{
				//Adding the element to the Result 
				finallist.add(val);
			}
			
			
			  //reset the max limit value 
		    	maxlimit = 0;
			    //Checking whether each linked list has reached its bound 
				for(int row = 0 ; row < qterms.length; ++row)
				{
				 
					if(pl[row] >= listlengths[row]) 
					{
						maxlimit++;
					}
					
				}
			
			  //System.out.println("maxlimit="+maxlimit);
			  //char ch = (char) System.in.read();
				
			}
	
		outwriter.flush();
		PrintUtil(finallist,daatandcount);
}
		 
	     
	
	/*
	 * Method: DAATOrQuery
	 * Description : It implements the DAAT OR algorithm 
	 * 
	 */
		
	public void DAATOrQuery(String[] qterms) throws IOException
	{
		
		
		//System.out.println("DAATOrQuery");
		outwriter.write("DaatOr") ;
		outwriter.write(System.getProperty("line.separator"));
	
		
		for(String qterm : qterms)
		{
			outwriter.write(qterm+" ") ;
			
		}
		outwriter.write(System.getProperty("line.separator"));
		outwriter.write("Results:") ;
		
		
		
		//Linked List for storing the final  DAAT results 
		LinkedList<Integer>finallist  = new LinkedList<Integer>();
		
		int [] pl = new int[qterms.length];
		
		
		//Pointers for each linked list are stored in an array .Initializing them 
		for(int i = 0 ;i<qterms.length ;++i)
		{
			pl[i] = 0 ;
			
		}
		
		//Elements for each iteration for comparison 
		ArrayList<Integer> ele = new ArrayList<Integer>();
		
				
		//Storing length of each linked list in an array 
		int[] listlengths = new int[qterms.length];
		for(int i= 0 ; i<qterms.length;++i)
		{
			listlengths[i] = daatlist.get(i).size();
			
		}
		
		int maxlimit  = 0 ;
		
		
		
		int comp = 0 ;
		int comppos = 0 ; 
		
		
		//picking the max element among all the linked lists for one iteration 
		//which is one time operation unlike in DAAT And 
		for(int row = 0 ; row <qterms.length ;++row)
		{
		
			if(pl[row]<listlengths[row])
			{
				
				if(daatlist.get(row).get(pl[row]) > comp)
				{
					comp = daatlist.get(row).get(pl[row]);
					comppos = row;
				}
			}
			
		}
		
		
		//Checking whether we have reached out of bound of every linked list 
		//Maxlimit is the number of linked lists which are out of bound 
		while(maxlimit < qterms.length) 
		{
			
			//System.out.println("while main");

		
			//Iterating across each term to check whether there is a same docid at a position in each row 
			
			//System.out.println("comppos= "+comppos);
			//System.out.println("comp= "+comp);
		
			for(int row = 0 ; row < qterms.length; ++row)
			{
			
			
				while(pl[row] < listlengths[row])
				{
					
					//System.out.println("row = "+row+" ,pl[row] ="+pl[row]);
					
					int temp = daatlist.get(row).get(pl[row]);
				    //System.out.println("temp = "+temp);
					
					
					    
					if(temp == comp )
					{
						if(comppos != row)
						{
							daatorcount++;
							
						}
						ele.add(temp) ;
						pl[row]++;
					}

					else if(temp < comp)
					{
						
						ele.add(temp) ;
						pl[row]++;
						daatorcount++;
						
					}
					else
					{
						daatorcount++;
						comp = temp;
						comppos = row;
						break;
					}
				//	char x =  (char) System.in.read();
					
					
			   }
				
			}
			
			//System.out.println("vals added to list");
			for(int i = 0 ;i<ele.size();++i)
			{
			   
				
				finallist.add(ele.get(i));
				//System.out.println("ele.get(i)"+ele.get(i));
			
			}
			
			ele.clear();
			
			
			maxlimit = 0;
		    //Checking whether each linked list has reached its bound 
			for(int row = 0 ; row < qterms.length; ++row)
			{
			 
				if(pl[row] >= listlengths[row]) 
				{
					maxlimit++;
				}
				
			}
			
			 // System.out.println("maxlimit="+maxlimit);
			 // char ch = (char) System.in.read();
				
			}
	
		PrintUtil(finallist,daatorcount);
		
		/*for(int i=0 ;i<finallist.size();++i)
		{
			
			 System.out.println(finallist.get(i));
		}*/
		outwriter.flush();
	}
	

	
}



    

