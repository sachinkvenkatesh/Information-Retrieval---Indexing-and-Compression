import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class DictEntry implements Serializable {

	private static final long serialVersionUID = 1L;
	transient String term;
	transient long totalTFreq;
	long df;
	List<PostingEntry> postingList;

	public DictEntry(String term) {
		this.term = term;
		totalTFreq = 0;
		df = 1;
		postingList = new ArrayList<>();
	}

	public void incrementDocFreq() {
		this.df++;
	}
	
	public void sortPostingList(){
		Collections.sort(this.postingList, new Comparator<PostingEntry>(){

			@Override
			public int compare(PostingEntry o1, PostingEntry o2) {
				return (int) (o1.d.docId-o2.d.docId);
			}
			
		});
	}

	public void addToPostingsList(Document d) {
		int size = postingList.size();
		if (size != 0 && postingList.get(size - 1).d.equals(d))
			postingList.get(size - 1).incrementTF();
		else {
			PostingEntry p = new PostingEntry(d);
			postingList.add(p);
			p.incrementTF();
			this.incrementDocFreq();
		}
	}

	public void calculateTotalFreq() {
		for (PostingEntry pe : postingList) {
			this.totalTFreq += pe.tfInDoc;
		}
	}

	public int hashcode() {
		return this.term.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o == null)
			return false;
		final DictEntry d = (DictEntry) o;
		return this.term.equals(d.term);
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException {
		Document doc1 = new Document(1);
		Document doc2 = new Document(2);

		PostingEntry pe1 = new PostingEntry(doc1);
		PostingEntry pe2 = new PostingEntry(doc2);

		DictEntry de = new DictEntry("First");
		de.postingList.add(pe1);
		de.incrementDocFreq();
		de.postingList.add(pe2);
		de.incrementDocFreq();

		DictEntry de2 = new DictEntry("Second");
		de2.postingList.add(pe1);
		de2.incrementDocFreq();
		de2.postingList.add(pe2);
		de2.incrementDocFreq();

	}
}
