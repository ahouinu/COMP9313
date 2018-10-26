/*
 * COMP9313 Big Data 18s2
 * TFPair, modified from StringPair in Lab4
 * @Author: Tianpeng Chen
 * @zid: z5176343
 * */


package comp9313.proj1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class TFPair implements WritableComparable<TFPair> {
	
	private String term;
	private String docID;
	
	public TFPair() {
		
	}
	
	public TFPair(String first, String second) {
		set(first, second);
	}
	
	public void set(String left, String right) {
		term = left;
		docID = right;
	}
	
	public void set(TFPair o) {
		term = o.getTerm();
		docID = o.getDocID();
	}
	
	public String getTerm() {
		return term;
	}
	
	public String getDocID() {
		return docID;
	}
	
	public void readFields(DataInput in)
		throws IOException {
		String[] strings = WritableUtils.readStringArray(in);
		term = strings[0];
		docID = strings[1];
	}
	
	public void write(DataOutput out)
		throws IOException {
		String[] strings = new String[] { term, docID };
		WritableUtils.writeStringArray(out, strings);
	}
	
	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append(term + " " + docID);
		return sb.toString();
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		
		TFPair that = (TFPair) o;
		
		if (term != null ? !term.equals(that.term) : that.term != null)
			return false;
		if (docID != null ? !docID.equals(that.docID) : that.docID != null)
			return false;
		
		return true;
	}
	
	@Override
	public int hashCode() {
		// only partition on term
		return term.hashCode();
	}
	
	private int compare(String s1, String s2) {
		if (s1 == null && s2 != null) {
			return -1;
		} else if (s1 != null && s2 == null) {
			return 1;
		} else if (s1 == null && s2 == null) {
			return 0;
		} else {
			return s1.compareTo(s2);
		}
	}
	
	public int numCompare(String s1, String s2) {
		
//		// "*" is the biggest
//		if (s1.equals("*")) {
//			return 1;
//		} else if (s2.equals("*")) {
//			return -1;
//		}
		
		int n1 = Integer.parseInt(s1);
		int n2 = Integer.parseInt(s2);
		
		if (n1 > n2) {
			return 1;
		} else if (n1 == n2) {
			return 0;
		} else {
			return -1;
		}
	}
	
	@Override
	public int compareTo(TFPair o) {
		int cmp = compare(term, o.getTerm());
		if (cmp != 0) 
			return cmp;
		// do numerical compare on docID
		return numCompare(docID, o.getDocID());
//		return compare(docID, o.getDocID());
	}
}
