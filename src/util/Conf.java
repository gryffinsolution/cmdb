package util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class Conf {
	private static final Logger LOG = LogManager.getLogger(Conf.class);
	private static String fileName = null;

	public Conf() {

	}

	@SuppressWarnings("static-access")
	public void setConfFile(String fileName) {
		File f = new File(fileName);
		if (!f.isFile()) {
			LOG.error("There is no configulation xml file");
			System.exit(0);
		}
		this.fileName = fileName;
	}

	@SuppressWarnings("static-access")
	public String getConfFile() {
		return this.fileName;
	}

	public int getSinglefValue(String key) {
		try {
			InputSource is = new InputSource(new FileReader(fileName));
			Document document = DocumentBuilderFactory.newInstance()
					.newDocumentBuilder().parse(is);
			XPath xpath = XPathFactory.newInstance().newXPath();
			String expression = "/flog/" + key;
			int value = Integer.parseInt((xpath.compile(expression)
					.evaluate(document)));
			LOG.trace(key + ":" + value);
			return value;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (XPathExpressionException e) {
			e.printStackTrace();
		}
		LOG.fatal("No config value about " + key);
		return 0;
	}

	public String getSingleString(String key) {
		try {
			InputSource is = new InputSource(new FileReader(fileName));
			Document document = DocumentBuilderFactory.newInstance()
					.newDocumentBuilder().parse(is);
			XPath xpath = XPathFactory.newInstance().newXPath();
			String expression = "/flog/" + key;
			String value = (xpath.compile(expression).evaluate(document));
			// LOG.trace(key + ":" + value);
			return value;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (XPathExpressionException e) {
			e.printStackTrace();
		}
		LOG.fatal("No config value about " + key);
		return null;
	}

	public String getDbURL() {
		try {
			InputSource is = new InputSource(new FileReader(fileName));
			Document document = DocumentBuilderFactory.newInstance()
					.newDocumentBuilder().parse(is);
			XPath xpath = XPathFactory.newInstance().newXPath();
			String expression = "/flog/main_db_url";
			String hostNmae = xpath.compile(expression).evaluate(document);
			LOG.trace("DBhostName:" + hostNmae);
			return hostNmae;

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (XPathExpressionException e) {
			e.printStackTrace();
		}
		return "localhost";
	}

	public String getTargetDbUrl() {
		try {
			InputSource is = new InputSource(new FileReader(fileName));
			Document document = DocumentBuilderFactory.newInstance()
					.newDocumentBuilder().parse(is);
			XPath xpath = XPathFactory.newInstance().newXPath();
			String expression = "/flog/result_db_url";
			String hostNmae = xpath.compile(expression).evaluate(document);
			LOG.trace("DBhostName:" + hostNmae);
			return hostNmae;

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (XPathExpressionException e) {
			e.printStackTrace();
		}
		return "localhost";
	}

	public ArrayList<String> getColumns() {
		ArrayList<String> columnList = new ArrayList<String>();
		try {
			LOG.trace(fileName);
			InputSource is = new InputSource(new FileReader(fileName));
			Document document = DocumentBuilderFactory.newInstance()
					.newDocumentBuilder().parse(is);
			XPath xpath = XPathFactory.newInstance().newXPath();
			String expression = "//columns/column";
			NodeList cols = (NodeList) xpath.compile(expression).evaluate(
					document, XPathConstants.NODESET);
			for (int idx = 0; idx < cols.getLength(); idx++) {
				String columnName = cols.item(idx).getAttributes().item(0)
						.getTextContent();
				columnList.add(columnName);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (XPathExpressionException e) {
			e.printStackTrace();
		}
		return columnList;
	}

	public void getColumnType(HashMap<String, String> columnMap,
			String columnName) {
		try {
			InputSource is = new InputSource(new FileReader(fileName));
			Document document = DocumentBuilderFactory.newInstance()
					.newDocumentBuilder().parse(is);
			XPath xpath = XPathFactory.newInstance().newXPath();
			String expression = "//columns/column[@name='" + columnName + "']";
			NodeList cols = (NodeList) xpath.compile(expression).evaluate(
					document, XPathConstants.NODESET);
			for (int idx = 0; idx < cols.getLength(); idx++) {
				String colName = cols.item(idx).getAttributes().item(0)
						.getTextContent();
				expression = "//columns/column[@name='" + columnName
						+ "']/type[@name='" + columnName + "']";
				String colType = xpath.compile(expression).evaluate(document);
				columnMap.put(colName, colType);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (XPathExpressionException e) {
			e.printStackTrace();
		}
	}
}