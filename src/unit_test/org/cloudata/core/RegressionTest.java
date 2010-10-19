package org.cloudata.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.HashSet;
import java.util.Map;

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TableSchema;
import org.htmlparser.Node;
import org.htmlparser.Parser;
import org.htmlparser.tags.LinkTag;
import org.htmlparser.tags.TitleTag;
import org.htmlparser.util.NodeIterator;
import org.htmlparser.util.NodeList;
import org.htmlparser.util.ParserException;



public class RegressionTest {
	static final String DELIMINATOR = "==P=>>>>=i===<<<<=T===>=A===<=!Junghoo!==>";
	static final String COLUMN_NAME_URL = "url";
	static final String COLUMN_NAME_PAGE = "page";
	static final String COLUMN_NAME_TITLE = "title";
	static final String COLUMN_NAME_OUTLINK = "outlink";


	public static void main(String[] args) {
		CloudataConf conf = new CloudataConf();
		if (args.length < 1) {
			System.err.println("Insufficient number of arguments");
			System.exit(1);
		}
		String tableName = args[0];

		try {
			File rawFile = new File("data/raw_data_file");

			if (rawFile.exists() == false) {
				System.err.println("Raw file does not exist");
				System.exit(1);
			}

			if (!CTable.existsTable(conf, tableName)) {
				TableSchema schema = new TableSchema(tableName);
				schema.setNumOfVersion(0);
				schema.addColumn(COLUMN_NAME_URL);
				schema.addColumn(COLUMN_NAME_PAGE);
				schema.addColumn(COLUMN_NAME_TITLE);
				schema.addColumn(COLUMN_NAME_OUTLINK);

				CTable.createTable(conf, schema);
			}

			CTable table = CTable.openTable(conf, tableName);

			if (table == null) {
				System.err.println("open table [" + tableName + "] is fail");
				System.exit(1);
			}

			RawDataParser parser = new RawDataParser(rawFile);
			parser.setHandler(new DataUploader(table));
			parser.start();

			try {
				Thread.sleep(2000);
			} catch(Exception e) {
			}

			parser.setHandler(new DataVerifier(table));
			parser.start();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	static class DataUploader implements RawDataParser.Handler {
		private CTable table;
		private int count = 0;

		public DataUploader(CTable table) {
			this.table = table;
			System.out.println("Start uploading");
		}

		public void process(String url, String page, String title, List<LinkInfo> linkInfoList) {
			boolean verbose = false;
			if (url.hashCode() == -581792209) {
				System.out.println("#### title[" + title + "]");
				verbose = true;
			}
			Row.Key rowKey = new Row.Key(String.valueOf(url.hashCode()));
			Row row = new Row(rowKey);
			row.addCell(COLUMN_NAME_URL, new Cell(Cell.Key.EMPTY_KEY, url.getBytes()));
			row.addCell(COLUMN_NAME_PAGE, new Cell(Cell.Key.EMPTY_KEY, page.getBytes()));
			if (title != null) {
				row.addCell(COLUMN_NAME_TITLE, new Cell(Cell.Key.EMPTY_KEY, title.getBytes()));
			}
			int linkCount = 0;
			for (LinkInfo eachLinkInfo : linkInfoList) {
				row.addCell(COLUMN_NAME_OUTLINK, new Cell(new Cell.Key(eachLinkInfo.link), eachLinkInfo.linkText
							.getBytes()));
				linkCount++;
				if (linkCount > 5000) {
					System.out.println("Too Many Links. added only 5000 links:" + url + ","
							+ linkInfoList.size());
					break;
				}
			}

			try {
				table.put(row);
			} catch (IOException e) {
				System.err.println("Skipping writing URL[" + url + "] due to " + e);
				writeSkippedURL(url);
				return;
			}

			count++;
			linkInfoList.clear();
			if (count % 1000 == 0) {
				System.out.println(count + " inserted, " + url);
			}
		}

		private void writeSkippedURL(String url) {
			try {
				FileOutputStream fos = new FileOutputStream("./skipped_url_list", true);
				PrintWriter out = new PrintWriter(fos);
				out.println(url);
				fos.close();
			} catch(IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

	static class DataVerifier implements RawDataParser.Handler {
		private CTable table;
		HashSet<String> skippedURLSet = new HashSet<String>();
		int count = 0;
		int startingCount;

		public DataVerifier(CTable table) {
			this(table, 0);
		}

		public DataVerifier(CTable table, int starting) {
			this.table = table;
			readSkippedURL();
			System.out.println("Start verification");
			startingCount = starting;
		}

		private void readSkippedURL() {
			try {
				FileInputStream fis = new FileInputStream("./skipped_url_list");
				BufferedReader br = new BufferedReader(new InputStreamReader(fis));
				String url = null;
				while((url = br.readLine()) != null) {
					skippedURLSet.add(url);	
				}
				fis.close();
			} catch(IOException e) {
				e.printStackTrace();
				System.exit(1);
			}

		}

		private void assertionFail(String url, String rowKey, String columnName, String expected, String actual) {
			System.err.println("Assertion fail. URL[" + url + "], RowKey[" + rowKey + "], Column[" + columnName + "]");
			System.err.println("Expected : [" + expected + "]");
			System.err.println("Actual : [" + actual + "]");
			System.exit(1);
		}

		private RowFilter createRowFilter(Row.Key rowKey) {
			RowFilter rowFilter = new RowFilter(rowKey);

			CellFilter urlFilter = new CellFilter(COLUMN_NAME_URL);
			urlFilter.setTimestamp(Long.MIN_VALUE, Long.MAX_VALUE);

			CellFilter titleFilter = new CellFilter(COLUMN_NAME_TITLE);
			titleFilter.setTimestamp(Long.MIN_VALUE, Long.MAX_VALUE);

			CellFilter pageFilter = new CellFilter(COLUMN_NAME_PAGE);
			pageFilter.setTimestamp(Long.MIN_VALUE, Long.MAX_VALUE);

			CellFilter outlinkFilter = new CellFilter(COLUMN_NAME_OUTLINK);
			outlinkFilter.setTimestamp(Long.MIN_VALUE, Long.MAX_VALUE);

			rowFilter.addCellFilter(urlFilter);
			rowFilter.addCellFilter(titleFilter);
			rowFilter.addCellFilter(pageFilter);
			rowFilter.addCellFilter(outlinkFilter);

			return rowFilter;
		}

		public void process(String url, String page, String title, List<LinkInfo> linkInfoList) {
			if (skippedURLSet.contains(url)) {
				System.out.println("URL [" + url + "] is skipped");
				return;
			}

			if (startingCount > count) {
				count++;
				return;
			}

			Row row = null;
			Row.Key rowKey = new Row.Key(String.valueOf(url.hashCode()));
			try {
				row = table.get(createRowFilter(rowKey));

				if (row == null) {
					System.err.println("NO data to be verified");
					System.exit(0);
				}
			} catch (IOException e) {
				System.err.println("Fail to get row. URL[" + url + "], rowkey[" + rowKey + "]");
				e.printStackTrace();
				System.exit(1);
			}

			if (contains(url, row.getCellList(COLUMN_NAME_URL)) == false) {
				assertionFail(url, rowKey.toString(), COLUMN_NAME_URL, url, toString(row.getCellList(COLUMN_NAME_URL)));
			}

			if (row.getCellList(COLUMN_NAME_TITLE) != null && row.getCellList(COLUMN_NAME_TITLE).size() > 0 
					&&  contains(title, row.getCellList(COLUMN_NAME_TITLE)) == false) {
				assertionFail(url, rowKey.toString(), COLUMN_NAME_TITLE, title, toString(row.getCellList(COLUMN_NAME_TITLE)));
			}

			if (contains(page, row.getCellList(COLUMN_NAME_PAGE)) == false) {
				assertionFail(url, rowKey.toString(), COLUMN_NAME_PAGE, "", "");
			}

			if (linkInfoList != null && linkInfoList.size() > 0) {
				checkLinkInfoList(url, row, linkInfoList);
			}

			//System.out.println("URL [" + url + "], rowKey [" + rowKey + "] is passed");
			System.out.print(".");

			count++;

			if (count % 80 == 0) {
				System.out.println("");
			}

			if (count % 1000 == 0) {
				System.out.println("");
				System.out.println(count + " is verified");
			}
		}

		private void checkLinkInfoList(String url, Row row, List<LinkInfo> linkInfoList) {
			Map<Cell.Key, Cell> cellMap = row.getCellMap(COLUMN_NAME_OUTLINK);

			if (cellMap == null || cellMap.size() == 0) {
				assertionFail(url, row.getKey().toString(), COLUMN_NAME_OUTLINK, "", "");
			}

			for(LinkInfo linkInfo : linkInfoList) {
				if (cellMap.containsKey(new Cell.Key(linkInfo.link)) == false) {
					assertionFail(url, row.getKey().toString(), COLUMN_NAME_OUTLINK, linkInfo.link, "");
				}

				Cell cell = cellMap.get(new Cell.Key(linkInfo.link));
				boolean found = false;
				for(Cell.Value value : cell.getValues()) {
					if (Arrays.equals(value.getBytes(), linkInfo.linkText.getBytes())) {
						found = true;
						break;
					}
				}

				if (!found) {
					assertionFail(url, row.getKey().toString(), COLUMN_NAME_OUTLINK, "CellKey[" + linkInfo.link + "], CellValue[" + linkInfo.linkText + "]", "");
				}
			}

		}

		private String toString(List<Cell> cellList) {
			StringBuilder sb = new StringBuilder();
			
			for(Cell cell : cellList) {
				sb.append("[");
				if (cell.getBytes() == null) {
					sb.append("null");
				} else {
					sb.append(new String(cell.getBytes()));
				}
				sb.append("] ");
			}

			return sb.toString();
		}

		private boolean contains(String value, List<Cell> cellList) {
			if (cellList == null) {
				System.err.println("cellList is null");
				return false;
			} 

			
			for(Cell.Value cellValue : cellList.get(0).getValues()) {
				if (cellValue == null) {
					System.err.println("cell value is null");
				}

				if (Arrays.equals(value.getBytes(), cellValue.getBytes())) {
					return true;
				}
			}

			return false;
		}    
	}

	static class RawDataParser {
		File rawFile;
		Handler handler;

		StringBuilder sb = new StringBuilder();
		private boolean startPage;
		private String url;
		private String title;
		private int emptyLineCount;
		private List<LinkInfo> linkInfos = new ArrayList<LinkInfo>();
		private int count = 0;

		public RawDataParser(File rawFile) {
			this.rawFile = rawFile;
		}

		public void setHandler(Handler handler) {
			this.handler = handler;
		}

		public void start() throws IOException {
			FileInputStream fis = new FileInputStream(rawFile);

			try {
				BufferedReader br = new BufferedReader(new InputStreamReader(fis));
				String line = null;
				while ((line = br.readLine()) != null) {
					processLine(line);
				}
			} finally {
				fis.close();
			}
		}

		public void processLine(String line) throws IOException {
			if (line.equals(DELIMINATOR)) {
				if (sb.length() > 0) {
					if (sb.length() > 1024 * 1024) {
						System.out.println("HTML too big:" + sb.length());
						return;
					}

					if (parseData() && handler != null) {
						handler.process(url, sb.toString(), title, linkInfos);
					}
				}
				startPage = true;
				sb.setLength(0);
				url = null;
				emptyLineCount = 0;
				linkInfos.clear();
				return;
			}

			if (!startPage) {
				return;
			}

			if (line == null || line.trim().length() == 0) {
				if (emptyLineCount <= 1) {
					emptyLineCount++;
					return;
				}
			}

			// header
			if (emptyLineCount <= 1) {
				if (line.startsWith("URL:")) {
					url = line.substring(5).trim();
				}
			} else {
				sb.append(line).append("\n");
			}
		}

		private boolean parseData() throws IOException {
			Parser parser = null;
			try {
				parser = new Parser(sb.toString());

				for (NodeIterator e = parser.elements(); e.hasMoreNodes();) {
					Node node = e.nextNode();
					parseTag(node);
				}
			} catch (org.htmlparser.util.ParserException e) {
				return false;
			} catch (Exception e1) {
				System.err.println("Error, ignore this data and try next ex[" + e1.getClass().getName() + "]");
				return false;
			}

			if (url == null) {
				return false;
			}

			return true;
		}

		private void parseTag(Node node) throws ParserException {
			if (node.getClass().equals(LinkTag.class)) {
				LinkTag tag = (LinkTag) node;
				String link = tag.extractLink();
				String linkText = tag.getLinkText();
				int position = tag.getStartPosition();
				//linkInfos.add(new LinkInfo(link, linkText, position));
				linkInfos.add(new LinkInfo(link, linkText));
			} else if (node.getClass().equals(TitleTag.class)) {
				TitleTag titleTag = (TitleTag) node;
				this.title = titleTag.toPlainTextString();
			}
			NodeList nodeList = node.getChildren();
			if (nodeList == null) {
				return;
			}

			for (NodeIterator e = nodeList.elements(); e.hasMoreNodes();) {
				Node childNode = e.nextNode();
				parseTag(childNode);
			}
		}

		static interface Handler {
			public void process(String url, String page, String title, List<LinkInfo> linkInfoList);
		}
	}

	static class LinkInfo {
		String link;
		String linkText;
		int position = 0;

		public LinkInfo(String link, String linkText) {
			this(link, linkText, 0);
		}
		
		public LinkInfo(String link, String linkText, int position) {
			this.link = link;
			if (linkText == null) {
				linkText = "";
			}
			this.linkText = linkText;
			this.position = position;
		}

		public boolean equals(Object obj) {
			if (obj instanceof LinkInfo == false) {
				return false;
			}

			LinkInfo target = (LinkInfo) obj;
			return target.link.equals(this.link) && target.linkText.equals(this.linkText) && target.position == this.position;
		}
	}
}