import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;

public class PostingEntry implements Serializable, Comparator<PostingEntry> {

	private static final long serialVersionUID = 1L;
	long tfInDoc;
	Document d;
	transient long gaps;

	public PostingEntry(Document d) {
		tfInDoc = 0;
		this.d = d;
	}

	public void incrementTF() {
		this.tfInDoc++;
		d.checkMaxTF(this.tfInDoc);
	}

	public boolean equals(Object o) {
		if (o == null)
			return false;
		final PostingEntry pe = (PostingEntry) o;
		return pe.d.equals(this.d);
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException {
		Document doc = new Document(1);
		PostingEntry pe = new PostingEntry(doc);
		pe.incrementTF();
		pe.incrementTF();
	}

	@Override
	public int compare(PostingEntry o1, PostingEntry o2) {
		return (int) (o1.d.docId-o2.d.docId);
	}

}
