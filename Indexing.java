import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

import java.util.Properties;

public class Indexing implements Serializable {

	private static final long serialVersionUID = 1L;
	private Set<String> stopWords;
	private TreeMap<String, DictEntry> stemDict;
	private TreeMap<String, DictEntry> lemmaDict;

	Porter p;
	StanfordCoreNLP pipeline;
	Properties prop;
	// Storing Document objects to get the docs having max maxTF and max docLen
	private ArrayList<Document> docsL;
	private ArrayList<Document> docsS;

	public Indexing(String stopWordFile) throws FileNotFoundException {
		stopWords = new HashSet<>();
		stemDict = new TreeMap<String, DictEntry>();
		lemmaDict = new TreeMap<>();
		File stp = new File(stopWordFile);
		Scanner in = new Scanner(stp);

		while (in.hasNextLine()) {
			stopWords.add(in.nextLine());
		}
		in.close();
		p = new Porter();

		prop = new Properties();
		prop.put("annotators", "tokenize, ssplit, pos, lemma");
		pipeline = new StanfordCoreNLP(prop);
		docsL = new ArrayList<>();
		docsS = new ArrayList<>();
	}

	/**
	 * 
	 * @param in:
	 *            Scanner - for the file to be indexed
	 * @param d:
	 *            Document - the document will store max_tf, docLen and docId
	 *            for the document being indexed
	 * @param dict:
	 *            TreeMap<String,DictEntry> - stores the term and its posting
	 *            list
	 * @param lemma:
	 *            boolean - true - lemmatization | false - stemming
	 * @throws IOException
	 */
	private void indexing(Scanner in, Document d, TreeMap<String, DictEntry> dict, boolean lemma) throws IOException {
		while (in.hasNextLine()) {
			String line = in.nextLine();
			String[] words = line.split("[\\s]+");
			for (int i = 0; i < words.length; i++) {
				d.incrementDocLen();

				if (words[i].matches("<[/]?[a-zA-Z]+>")
						|| words[i].matches("<[/]?[a-zA-Z]+ ([a-zA-Z]+[=[\"a-zA-Z0-9#$]+])+>")) {
					continue;
				}
				String tkn = words[i].trim();
				tkn = tkn.replace("-", "");
				if (tkn.matches("[A-Z.]{2,}")) {
					tkn.replace(".", "");
				} else
					tkn = tkn.toLowerCase();
				tkn = tkn.replace("'s", "");
				tkn = tkn.replaceAll("[\\$\\*\\+\\:\\?=.,/()'\"\\d\\s]+", "");
				if (tkn.equals("") || stopWords.contains(tkn))
					continue;
				String key = null;

				if (lemma)
					key = lemmatize(tkn);// find lemma
				else
					key = p.stripAffixes(tkn); // find stem

				if (key.equals("") || key.equals(" "))
					continue;

				// add to the dictionary/treeMap
				if (dict.containsKey(key)) {
					DictEntry de = dict.get(key);
					// add the docID(Document class) to the posting list
					de.addToPostingsList(d);
				} else {
					// create new entry to be added to the dictionary
					DictEntry de = new DictEntry(key);
					// add the docID(Document class) to the posting list
					de.addToPostingsList(d);
					dict.put(key, de);
				}
			}
		}
		if (lemma)
			docsL.add(d);
		else
			docsS.add(d);// add Document to list containing docs that are
							// processed
		in.close();
	}

	/**
	 * Lemmatization
	 * 
	 * @param term:
	 *            String
	 * @return - lemma of the term
	 */
	private String lemmatize(String term) {
		Annotation annotation = new Annotation(term);
		pipeline.annotate(annotation);
		List<CoreMap> sentences = annotation.get(CoreAnnotations.SentencesAnnotation.class);
		String lemma = "";
		for (CoreMap coreMap : sentences) {
			for (CoreLabel coreLabel : coreMap.get(CoreAnnotations.TokensAnnotation.class)) {
				lemma = coreLabel.get(CoreAnnotations.LemmaAnnotation.class);
			}
		}
		return lemma;
	}

	/**
	 * Compression of Dictionary, posting list
	 * 
	 * @param k:
	 *            int - block size
	 * @param dict:
	 *            TreeMap - dictionary having term and posting list
	 * @param gamma:
	 *            boolean - true - gamma code | false - delta code
	 * @throws IOException
	 */
	public byte[] compression(int k, TreeMap<String, DictEntry> dict, ArrayList<Long> termPtr, boolean gamma)
			throws IOException {
		System.out.println("----------Compression Started----------");
		Set<Long> docIds = new HashSet<>();
		int countKeys = 0;
		int j = 0;
		long prev; // to find gaps
		DictEntry de;
		PostingEntry pe;
		List<PostingEntry> l;
		ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
		ByteArrayOutputStream byteArrayDocs = new ByteArrayOutputStream();
		ArrayList<byte[]> postingBlock = new ArrayList<>();
		for (String str : dict.navigableKeySet()) {
			countKeys++;
			prev = 0;
			de = dict.get(str);
			l = de.postingList;// get posting list for the term str
			byte[] df;
			if (gamma)
				df = convertToBits(gammaCode(de.df));
			else
				df = convertToBits(deltaCode(de.df));
			byteArray.write(df);

			// for each posting entry in the list
			for (int i = 0; i < l.size(); i++) {
				pe = l.get(i);
				// find gaps
				pe.gaps = pe.d.docId - prev;
				prev = pe.d.docId;
				// compress gaps and tf in doc and concatenate them
				byte[] gap;
				byte[] tf;
				if (gamma) {
					gap = convertToBits(gammaCode(pe.gaps));
					tf = convertToBits(gammaCode(pe.tfInDoc));
				} else {
					gap = convertToBits(deltaCode(pe.gaps));
					tf = convertToBits(deltaCode(pe.tfInDoc));
				}
				byteArray.write(gap);
				byteArray.write(tf);

				// Compressing the Doc details - maxTF and docLen
				if (!(docIds.contains(pe.d.docId))) {
					if (gamma) {
						byteArrayDocs.write(convertToBits(gammaCode(pe.d.doclen)));
						byteArrayDocs.write(convertToBits(gammaCode(pe.d.max_tf)));
					} else {
						byteArrayDocs.write(convertToBits(deltaCode(pe.d.doclen)));
						byteArrayDocs.write(convertToBits(deltaCode(pe.d.max_tf)));
					}
				}
			}
			// byteArray.write("\n");
			postingBlock.add(byteArray.toByteArray());
			// Store the k compressed posting lists
			if (countKeys % k == 0) {
				if (gamma)
					byteArray.write(convertToBits(gammaCode(termPtr.get(j))));
				else
					byteArray.write(convertToBits(deltaCode(termPtr.get(j))));
				j++;
			}
		}
		// combine/concatenate the posting list byte array and doc details byte
		// array
		byteArray.write(byteArrayDocs.toByteArray());
		System.out.println("----------Compression Finished----------");
		return byteArray.toByteArray();
	}

	public static void writeCompressed(String path, String dictCompr, byte[] postingCompr) throws IOException {
		File f = new File(path);
		RandomAccessFile raf = new RandomAccessFile(f, "rw");
		System.out.println("----------Writing Compressed Data----------");
		raf.writeBytes(dictCompr);
		raf.write(postingCompr);
		raf.close();
		System.out.println("--------------Writing Finished-------------");
	}

	public static void writeUncompressed(String path, TreeMap<String, DictEntry> dict) throws IOException {
		PrintWriter pw = new PrintWriter(path);
		System.out.println("----------Writing Uncompressed Data----------");
		for (String str : dict.navigableKeySet()) {
			DictEntry de = dict.get(str);
			pw.print("(" + str + "," + de.df + ")->");
			List<PostingEntry> l = de.postingList;
			PostingEntry pe;
			long prev = 0;
			for (int i = 0; i < l.size(); i++) {
				pw.print("->");
				pe = l.get(i);
				pe.gaps = pe.d.docId - prev;
				prev = pe.d.docId;
				pw.print(pe.tfInDoc + "," + pe.d.docId + "," + pe.d.doclen + "," + pe.d.max_tf);
			}
			pw.print("\n");
		}
		pw.close();
		System.out.println("---------------Writing Finished--------------");
	}

	/**
	 * 
	 * @param d
	 * @param termPtr
	 * @param k
	 * @return
	 */
	public String blockCompression(TreeMap<String, DictEntry> d, ArrayList<Long> termPtr, int k) {
		long i = 0;
		StringBuilder sb = new StringBuilder();
		long totalCount = 0;
		for (String str : d.keySet()) {
			if (i % k == 0) {
				termPtr.add(totalCount);
			}
			i++;
			sb.append(str.length());
			sb.append(str);
			totalCount += str.length();
		}
		return sb.toString();
	}

	/**
	 * 
	 * @param d
	 * @param termPtr
	 * @param k
	 * @return
	 */
	public String frontCodingCompression(TreeMap<String, DictEntry> d, ArrayList<Long> termPtr, int k) {
		StringBuilder sb = new StringBuilder();
		String commonPrefix;
		int lenCP;
		long i = 0;
		long totalCount = 0;
		ArrayList<String> block = new ArrayList<>();
		for (String key : d.keySet()) {
			i++;
			if (!(i % k == 0)) {
				block.add(key);
				continue;
			}
			termPtr.add(totalCount);
			lenCP = 0;
			commonPrefix = longestCommonPrefix(block);
			if (!commonPrefix.equals("")) {
				lenCP = commonPrefix.length();
			}
			Iterator<String> itr = block.iterator();
			if (itr.hasNext()) {
				String str = itr.next();
				sb.append(str.length());
				sb.append(commonPrefix);
				sb.append("*");
				sb.append(str.substring(lenCP, str.length()));
				totalCount += str.length() + commonPrefix.length() + 1 + (str.length() - lenCP);
			}
			while (itr.hasNext()) {
				String str = itr.next();
				sb.append(str.length() - lenCP);
				sb.append("$");
				sb.append(str.substring(lenCP, str.length()));
				totalCount += Integer.toString(str.length() - lenCP).length() + 1 + (str.length() - lenCP);
			}
			block = new ArrayList<>();
		}
		return sb.toString();
	}

	/**
	 * 
	 * @param words
	 * @return
	 */
	private String longestCommonPrefix(ArrayList<String> words) {
		if (words.size() == 0)
			return null;
		StringBuilder sb;
		String commonPrefix = words.get(0);
		for (int i = 1; i < words.size(); i++) {
			sb = new StringBuilder();
			String str = words.get(i);
			for (int j = 0; j < str.length() && j < commonPrefix.length(); j++) {
				if (str.charAt(j) == commonPrefix.charAt(j)) {
					sb.append(str.charAt(j));
					continue;
				}
				break;
			}
			commonPrefix = sb.toString();
		}
		return commonPrefix;
	}

	/**
	 * 
	 * @param num
	 * @return
	 */
	public static String gammaCode(long num) {
		String bin = Long.toString(num, 2);
		String offset;
		int lenOffset;
		if (bin.length() == 1) {
			offset = null;
			lenOffset = 0;
		} else {
			offset = bin.substring(1);
			lenOffset = offset.length();
		}
		StringBuilder sb = new StringBuilder();
		while (lenOffset > 0) {
			sb.append("1");
			lenOffset--;
		}
		sb.append("0");

		String unaryCodeofLength = sb.toString();
		String gammaCode;
		if (offset == null)
			gammaCode = unaryCodeofLength;
		else
			gammaCode = unaryCodeofLength.concat(offset);
		return gammaCode;
	}

	/**
	 * 
	 * @param num
	 * @return
	 */
	public static String deltaCode(long num) {
		String bin = Long.toString(num, 2);
		int binLen = bin.length();
		String gammaCode = gammaCode(binLen);
		String offset;
		String deltaCode;
		if (bin.length() == 1) {
			offset = null;
			deltaCode = gammaCode;
		} else {
			offset = bin.substring(1);
			deltaCode = gammaCode.concat(offset);
		}
		return deltaCode;
	}

	/**
	 * 
	 * @param s
	 * @return
	 */
	private static byte[] convertToBits(String s) {
		BitSet bitSet = new BitSet(s.length());
		for (int i = 0; i < s.length(); i++) {
			Boolean value = s.charAt(i) == '1' ? true : false;
			bitSet.set(i, value);
		}
		return bitSet.toByteArray();
	}

	/**
	 * 
	 * @param foldername
	 * @throws IOException
	 */
	public void stemIndex(File foldername) throws IOException {
		File[] fnames = foldername.listFiles();
		System.out.println("----------Indexing Started----------");
		for (int i = 0; i < fnames.length; i++) {
			Scanner in = new Scanner(fnames[i]);
			String[] filename = fnames[i].getName().split("(?=\\d)(?<!\\d)");
			Document d = new Document(Long.parseLong(filename[1]));
			indexing(in, d, stemDict, false);
		}
		for (String str : stemDict.keySet()) {
			stemDict.get(str).sortPostingList();
		}
		calculateTotalTF(stemDict.values());
		System.out.println("----------Indexing Finished----------");
	}

	/**
	 * 
	 * @param foldername
	 * @throws IOException
	 */
	public void lemmaIndex(File foldername) throws IOException {
		File[] fnames = foldername.listFiles();
		System.out.println("----------Indexing Started----------");
		for (int i = 0; i < fnames.length; i++) {
			Scanner in = new Scanner(fnames[i]);
			String[] filename = fnames[i].getName().split("(?=\\d)(?<!\\d)");
			Document d = new Document(Long.parseLong(filename[1]));
			indexing(in, d, lemmaDict, true);
		}
		for (String str : lemmaDict.keySet()) {
			lemmaDict.get(str).sortPostingList();
		}
		calculateTotalTF(lemmaDict.values());
		System.out.println("----------Indexing Finished----------");
	}

	/**
	 * 
	 * @param values
	 */
	private void calculateTotalTF(Collection<DictEntry> values) {
		for (DictEntry de : values) {
			de.calculateTotalFreq();
		}
	}

	/**
	 * 
	 * @param l
	 * @return
	 * @throws IOException
	 */
	private long sizeOfInvertedList(List<PostingEntry> l) throws IOException {
		PrintWriter p = new PrintWriter("temp");
		for (PostingEntry pe : l) {
			p.print(pe.d.docId);
			p.print(pe.tfInDoc);
			p.print(pe.d.docId);
			p.print(pe.d.max_tf);
		}
		p.close();
		File f = new File("temp");
		long size = f.length();
		f.delete();
		return size;
	}

	/**
	 * 
	 * @param words
	 * @param dict
	 * @param lemma
	 * @throws IOException
	 */
	public void printStats(String[] words, TreeMap<String, DictEntry> dict, boolean lemma) throws IOException {
		String word = null;
		System.out.println("Terms  \t|DF\t|TF\t|SizeOfInvertedList");
		System.out.println("-------------------------------------");
		for (int i = 0; i < words.length; i++) {
			if (lemma)
				word = lemmatize(words[i].toLowerCase());
			else
				word = p.stripAffixes(words[i].toLowerCase());
			DictEntry de = dict.get(word);
			long df = de.df;
			long tf = de.totalTFreq;
			long sizeInvList = sizeOfInvertedList(de.postingList);
			System.out.println(words[i] + " \t|" + df + "\t|" + tf + "\t|" + sizeInvList);
		}
		System.out.println("-------------------------------------");
	}

	/**
	 * 
	 * @param dict
	 */
	private void dfMaxMin(TreeMap<String, DictEntry> dict) {
		long firstDF = dict.firstEntry().getValue().df;
		long max = firstDF;
		ArrayList<String> maxList = new ArrayList<>();
		maxList.add(dict.firstKey());
		long min = firstDF;
		ArrayList<String> minList = new ArrayList<>();
		minList.add(dict.firstKey());
		for (String str : dict.keySet()) {
			long df = dict.get(str).df;
			if (df <= min) {
				if (df == min)
					minList.add(str);
				else {
					minList = new ArrayList<>();
					minList.add(str);
				}
				min = df;
			}
			if (df >= max) {
				if (df == max)
					maxList.add(str);
				else {
					maxList = new ArrayList<>();
					maxList.add(str);
				}
				max = df;
			}
		}
		System.out.print("Term having largest DF: ");
		for (String maxS : maxList) {
			System.out.print(maxS + ",");
		}
		System.out.println();
		System.out.print("Term having lowest DF: ");
		for (String minS : minList) {
			System.out.print(minS + ",");
		}
		System.out.println();
	}

	/**
	 * 
	 */
	private void findMaxMinDF() {
		System.out.println("Index_Version1");
		dfMaxMin(lemmaDict);
		System.out.println("Index_Version2");
		dfMaxMin(stemDict);
	}

	/**
	 * 
	 */
	private void maxTF() {
		long max = docsL.get(0).max_tf;
		long docID = docsL.get(0).docId;
		for (int i = 1; i < docsL.size(); i++) {
			if (docsL.get(i).max_tf > max) {
				max = docsL.get(i).max_tf;
				docID = docsL.get(i).docId;
			}
		}
		System.out.println("Document with max max_tf (Index_Version1): " + docID);
		max = docsS.get(0).max_tf;
		docID = docsS.get(0).docId;
		for (int i = 1; i < docsS.size(); i++) {
			if (docsS.get(i).max_tf > max) {
				max = docsS.get(i).max_tf;
				docID = docsS.get(i).docId;
			}
		}
		System.out.println("Document with max max_tf (Index_Version2): " + docID);
	}

	/**
	 * 
	 */
	private void maxDocLen() {
		long max = docsL.get(0).doclen;
		long docID = docsL.get(0).docId;
		for (int i = 1; i < docsL.size(); i++) {
			if (docsL.get(i).doclen > max) {
				max = docsL.get(i).doclen;
				docID = docsL.get(i).docId;
			}
		}
		System.out.println("Document with max doc len: " + docID);
	}

	/**
	 * 
	 * @param filename
	 * @return
	 */
	private static long getFileSize(String filename) {
		File f = new File(filename);
		return f.length();
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException {

//		File f = new File("G:\\InformationRetrieval\\Assignment1\\IR_Assignment1\\");
		 File f = new File(args[0]);
//		String path = "G:\\InformationRetrieval\\Assignment2\\Out";
		 String path = args[1];
//		Indexing idx = new Indexing("G:\\InformationRetrieval\\Assignment2\\IR2\\stopwords");
		 Indexing idx = new Indexing(args[2]);
//		int k = 8;
		 int k = Integer.parseInt(args[3]);

		Timer t1 = new Timer();
		Timer t2 = new Timer();
		Timer t3 = new Timer();
		Timer t4 = new Timer();
		ArrayList<Long> termPtrL = new ArrayList<>();
		ArrayList<Long> termPtrS = new ArrayList<>();

		t1.start();
		idx.lemmaIndex(f);
		t1.end();
		t3.start();
		String dictBlockCodeCompressed = idx.blockCompression(idx.lemmaDict, termPtrL, k);
		byte[] lemmaPostingCompressed = idx.compression(k, idx.lemmaDict, termPtrL, true);
		t3.end();
		t2.start();
		idx.stemIndex(f);
		t2.end();
		t4.start();
		String dictFrontCodeCompressed = idx.frontCodingCompression(idx.stemDict, termPtrS, k);
		byte[] stemPostingCompressed = idx.compression(k, idx.stemDict, termPtrS, false);
		t4.end();

		System.out.println("Running Time for Index_Version1 uncompressed(in ms): " + t1.elapsedTime);
		System.out.println("Running Time for Index_Version1 compressed(in ms): " + t3.elapsedTime);
		System.out.println("Running Time for Index_Version2 uncompressed(in ms): " + t2.elapsedTime);
		System.out.println("Running Time for Index_Version2 compressed(in ms): " + t4.elapsedTime);

		System.out.println("Number of inverted lists in Index Version1: " + idx.lemmaDict.size());
		System.out.println("Number of inverted lists in Index Version2: " + idx.stemDict.size());

		// LemmaIndex - uncompressed
		writeUncompressed(path + "Index_Version1.uncompress", idx.lemmaDict);
		// stemIndex - uncompressed
		writeUncompressed(path + "Index_Version2.uncompress", idx.stemDict);
		// LemmaIndex - compressed
		writeCompressed(path + "Index_Version1.compressed", dictBlockCodeCompressed, lemmaPostingCompressed);
		// stemIndex - compressed
		writeCompressed(path + "Index_Version2.compressed", dictFrontCodeCompressed, stemPostingCompressed);

		System.out.println(
				"Size of Index_Version1 uncompressed(in bytes): " + getFileSize(path + "Index_Version1.uncompress"));
		System.out.println(
				"Size of Index_Version1 compressed(in bytes): " + getFileSize(path + "Index_Version1.compressed"));
		System.out.println(
				"Size of Index_Version2 uncompressed(in bytes): " + getFileSize(path + "Index_Version2.uncompress"));
		System.out.println(
				"Size of Index_Version2 compressed(in bytes): " + getFileSize(path + "Index_Version2.compressed"));
		System.out.println();

		System.out.println("Stats for given terms:");
		String[] terms = { "Reynolds", "NASA", "Prandtl", "flow", "pressure", "boundary", "shock" };
		System.out.println("Index_Version1");
		idx.printStats(terms, idx.lemmaDict, true);
		System.out.println("Index_Version2");
		idx.printStats(terms, idx.stemDict, false);

		System.out.println();
		String word = "NASA";
		int j = 3;
		DictEntry de = idx.stemDict.get(idx.p.stripAffixes(word.toLowerCase()));
		System.out.println("DocFreq for term NASA: " + de.df);
		System.out.println("First " + j + " entries in posting list of the term " + word + " :");
		System.out.println("DocID\t|TF\t|Doclen\t|Max_TF");
		System.out.println("-----------------------------");
		for (PostingEntry pe : de.postingList) {
			if (j == 0)
				break;
			j--;
			System.out.println(pe.d.docId + "\t|" + pe.tfInDoc + "\t|" + pe.d.doclen + "\t|" + pe.d.max_tf);
		}
		System.out.println("-----------------------------");
		System.out.println();
		idx.findMaxMinDF();
		idx.maxTF();
		idx.maxDocLen();
	}
}
