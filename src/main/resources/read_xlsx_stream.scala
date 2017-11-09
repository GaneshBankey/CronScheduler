import java.io.InputStream

import java.util.Iterator

import org.apache.poi.xssf.eventusermodel.XSSFReader

import org.apache.poi.xssf.model.SharedStringsTable

import org.apache.poi.openxml4j.opc.OPCPackage

import org.apache.poi.xssf.usermodel.XSSFRichTextString

import org.xml.sax.Attributes

import org.xml.sax.ContentHandler

import org.xml.sax.InputSource

import org.xml.sax.SAXException

import org.xml.sax.XMLReader

import org.xml.sax.helpers.DefaultHandler

import org.xml.sax.helpers.XMLReaderFactory


import ExampleEventUserModel._

//remove if not needed
import scala.collection.JavaConversions._

/**
  * See org.xml.sax.helpers.DefaultHandler javadocs
  */
class SheetHandler (var sst: org.apache.poi.xssf.model.SharedStringsTable) extends org.xml.sax.helpers.DefaultHandler {

  var lastContents: String = _

  var nextIsString: Boolean = _

  override def startElement(uri: String, localName: String, name: String, attributes: org.xml.sax.Attributes): Unit = {
// c => cell
    if (name.==("c")) {
// Print the cell reference
      print(attributes.getValue("r") + " - ")
// Figure out if the value is an index in the SST
      val cellType: String = attributes.getValue("t")
      nextIsString = if (cellType != null && cellType.==("s")) true else false
    }
// Clear contents cache
    lastContents = ""
  }

 override  def endElement(uri: String, localName: String, name: String): Unit = {
// Do now, as characters() may be called more than once
    if (nextIsString) {
      val idx: Int = java.lang.Integer.parseInt(lastContents)
      lastContents = new org.apache.poi.xssf.usermodel.XSSFRichTextString(sst.getEntryAt(idx)).toString
      nextIsString = false
    }
// Output after we've seen the string contents
    if (name.==("v")) {
      println(lastContents)
    }
  }
// Process the last contents as required.
// v => contents of a cell
// Process the last contents as required.
// v => contents of a cell

 override  def characters(ch: Array[Char], start: Int, length: Int): Unit = {
    lastContents += new String(ch, start, length)
  }

}


class ExampleEventUserModel {

  def processOneSheet(filename: String): Unit = {
    val pkg: org.apache.poi.openxml4j.opc.OPCPackage = org.apache.poi.openxml4j.opc.OPCPackage.open(filename)
    val r: org.apache.poi.xssf.eventusermodel.XSSFReader = new org.apache.poi.xssf.eventusermodel.XSSFReader(pkg)
    val sst: org.apache.poi.xssf.model.SharedStringsTable = r.getSharedStringsTable
    val parser: org.xml.sax.XMLReader = fetchSheetParser(sst)
// Normally it's of the form rId# or rSheet#
    val sheet2: java.io.InputStream = r.getSheet("rId2")
    val sheetSource: org.xml.sax.InputSource = new org.xml.sax.InputSource(sheet2)
    parser.parse(sheetSource)
    sheet2.close()
  }
// To look up the Sheet Name / Sheet Order / rID,
//  you need to process the core Workbook stream.
// To look up the Sheet Name / Sheet Order / rID,
//  you need to process the core Workbook stream.

  def processAllSheets(filename: String): Unit = {
    val pkg: org.apache.poi.openxml4j.opc.OPCPackage = org.apache.poi.openxml4j.opc.OPCPackage.open(filename)
    val r: org.apache.poi.xssf.eventusermodel.XSSFReader = new org.apache.poi.xssf.eventusermodel.XSSFReader(pkg)
    val sst: org.apache.poi.xssf.model.SharedStringsTable = r.getSharedStringsTable
    val parser: org.xml.sax.XMLReader = fetchSheetParser(sst)
    val sheets: java.util.Iterator[java.io.InputStream] = r.getSheetsData
    while (sheets.hasNext) {
      println("Processing new sheet:\n")
      val sheet: java.io.InputStream = sheets.next()
      val sheetSource: org.xml.sax.InputSource = new org.xml.sax.InputSource(sheet)
      parser.parse(sheetSource)
      sheet.close()
      println("")
    }
  }

  def fetchSheetParser(sst: org.apache.poi.xssf.model.SharedStringsTable): org.xml.sax.XMLReader = {
    val parser: org.xml.sax.XMLReader =
      org.xml.sax.helpers.XMLReaderFactory.createXMLReader("org.apache.xerces.parsers.SAXParser")
    val handler: org.xml.sax.ContentHandler = new SheetHandler(sst)
    parser.setContentHandler(handler)
    parser
  }

}


val example: ExampleEventUserModel = new ExampleEventUserModel()
example.processAllSheets("/home/hduser/my_jars/StudentInfo.xlsx")