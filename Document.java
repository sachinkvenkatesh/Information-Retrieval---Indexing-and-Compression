import java.io.IOException;
import java.io.Serializable;

public class Document implements Serializable {

	private static final long serialVersionUID = 1L;
	transient public static long docCount = 0;
	long docId;
	long doclen;
	long max_tf;

	public Document(long num) {
		docId = num;
		doclen = 0;
		max_tf = 0;
	}

	public void checkMaxTF(long tf) {
		if (tf > this.max_tf)
			max_tf = tf;
		return;
	}

	public void incrementDocLen() {
		this.doclen++;
	}

	@Override
	public boolean equals(Object o) {
		if (o == null)
			return false;
		final Document newD = (Document) o;
		return newD.docId == this.docId;
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException {
		Document d = new Document(1);
		System.out.println(d.docId);
		d.incrementDocLen();
		d.checkMaxTF(3);
		System.out.println(d.doclen);
		System.out.println(d.max_tf);
		Document d1 = new Document(2);
		System.out.println("new doc:" + d1.docId);
		d1.incrementDocLen();
		d.incrementDocLen();
		d1.checkMaxTF(4);
		d1.checkMaxTF(1);
		System.out.println(d1.doclen);
		System.out.println(d1.max_tf);
		System.out.println(d.doclen);
	}
}
