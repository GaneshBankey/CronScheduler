import util.control.Breaks._
import java.util.ArrayList
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.PrintStream
import javax.xml.parsers.ParserConfigurationException
import javax.xml.parsers.SAXParser
import javax.xml.parsers.SAXParserFactory
import org.apache.poi.openxml4j.exceptions.OpenXML4JException
import org.apache.poi.openxml4j.opc.OPCPackage
import org.apache.poi.openxml4j.opc.PackageAccess
import org.apache.poi.ss.usermodel.BuiltinFormats
import org.apache.poi.ss.usermodel.DataFormatter
import org.apache.poi.xssf.eventusermodel.ReadOnlySharedStringsTable
import org.apache.poi.xssf.eventusermodel.XSSFReader
import org.apache.poi.xssf.model.StylesTable
import org.apache.poi.xssf.usermodel.XSSFCellStyle
import org.apache.poi.xssf.usermodel.XSSFRichTextString
import org.xml.sax.Attributes
import org.xml.sax.ContentHandler
import org.xml.sax.InputSource
import org.xml.sax.SAXException
import org.xml.sax.XMLReader
import org.xml.sax.helpers.DefaultHandler

import org.apache.spark.sql.types.{StructType, StructField, StringType}
import collection.JavaConversions._

object xssfDataType extends Enumeration {
    type xssfDataType = Value
    val BOOL, ERROR, FORMULA, INLINESTR, SSTINDEX, NUMBER = Value
}
import xssfDataType._
var countrows = 0
var headersName = new ArrayList[String]()
val rowDataRDD = new ArrayList[org.apache.spark.sql.Row]()

class StreamXSSFSheetHandler (var stylesTable : StylesTable, var sharedStringsTable : ReadOnlySharedStringsTable, var minColumnCount : Integer, var output : ArrayList[Any]) extends DefaultHandler {
        // Set when V start element is seen
        var vIsOpen = false
        var nextDataType = xssfDataType.NUMBER
 
        // Used to format numeric cell values.
        var formatIndex = 0
        var formatString = ""
        val formatter = new DataFormatter()
 
        var thisColumn = -1
        // The last column printed to the output stream
        var lastColumnNumber = -1
        var minColumnNumber = 0
 
        // Gathers characters as they are seen.
        var value = new StringBuffer()
    
        override def startElement(uri: String, localName: String, name: String, attributes: Attributes): Unit = {
            
            if (name.==("inlineStr") || name.==("v")) {
                vIsOpen = true
                // Clear contents cache
                value.setLength(0)
            } else if (name.==("c")){
                
                var r = attributes.getValue("r");
                var firstDigit = -1;
                breakable {
                    for (c <- 0 to r.length()-1){
                        if (Character.isDigit(r.charAt(c))) {
                            firstDigit = c
                            break
                        }
                    }
                }
                
                thisColumn = nameToColumn(r.substring(0, firstDigit));

                // Set up defaults.
                this.nextDataType = xssfDataType.NUMBER;
                this.formatIndex = -1;
                this.formatString = null;
                var cellType = attributes.getValue("t");
                var cellStyleStr = attributes.getValue("s");
                if (cellType.==("b"))
                    nextDataType = xssfDataType.BOOL;
                else if (cellType.==("e"))
                    nextDataType = xssfDataType.ERROR;
                else if (cellType.==("inlineStr"))
                    nextDataType = xssfDataType.INLINESTR;
                else if (cellType.==("s"))
                    nextDataType = xssfDataType.SSTINDEX;
                else if (cellType.==("str"))
                    nextDataType = xssfDataType.FORMULA;
                else if (cellStyleStr != null) {
                    // It's a number, but almost certainly one
                    //  with a special style or format 
                    var styleIndex = Integer.parseInt(cellStyleStr);
                    var style = stylesTable.getStyleAt(styleIndex);
                    this.formatIndex = style.getDataFormat();
                    this.formatString = style.getDataFormatString();
                    if (this.formatString == null)
                        this.formatString = BuiltinFormats.getBuiltinFormat(this.formatIndex);
                }
                
            }
    }
    override def endElement(uri: String, localName: String, name: String): Unit = {
        var thisStr = ""
        
        // v => contents of a cell
        if (name.==("v")) {
            nextDataType match {
                case xssfDataType.BOOL => {
                    var first = value.charAt(0)
                    thisStr =  if(first == '0') "FALSE" else "TRUE"
                }                                    
                case xssfDataType.ERROR => {
                    thisStr = '"' + value.toString() + '"'   
                }                    
                case xssfDataType.FORMULA => {
                    // A formula could result in a string value,
                    // so always add double-quote characters.
                    thisStr = '"' + value.toString() + '"'  
                }                                     
                case xssfDataType.INLINESTR => {
                    // TODO: have seen an example of this, so it's untested.
                    var rtsi = new XSSFRichTextString(value.toString())
                    thisStr = '"' + rtsi.toString() + '"'   
                }                                        
                case xssfDataType.SSTINDEX => {
                    var sstIndex = value.toString()
                    var idx = Integer.parseInt(sstIndex)
                    var rtss = new XSSFRichTextString(sharedStringsTable.getEntryAt(idx))
                    thisStr = '"' + rtss.toString() + '"'
                }
                case xssfDataType.NUMBER => {
                    var n = value.toString()
                    if (this.formatString != null)
                        thisStr = formatter.formatRawCellContents(java.lang.Double.parseDouble(n), this.formatIndex, this.formatString)
                    else
                        thisStr = n
                }                                        
            }
            if (lastColumnNumber == -1) {
                lastColumnNumber = 0
            }
            if((thisColumn - lastColumnNumber) > 1 || output.size() == 0) {
                //if previous cell is empty                
                for (i <- lastColumnNumber until thisColumn) {
                    output.add("")
                }
            }
            //Adding the Cell Value in the list
            if(countrows == 0 ){
                headersName.add(thisStr);
            }else{
                output.add(thisStr)    
            }
            
            
            
            if (thisColumn > -1) {
                    lastColumnNumber = thisColumn 
            }
            
        }else if (name.==("row")){
            
            if(countrows == 0 ) {
                minColumnNumber = headersName.length
            }
            if (lastColumnNumber == -1) {
                lastColumnNumber = 0
            }
            for (i <- lastColumnNumber until minColumnNumber) {
                output.add("")
            }
            
            if(countrows > 0 ){
                rowDataRDD.add(org.apache.spark.sql.Row.fromSeq(output))
            }
            output.clear()
            countrows += 1            
            
            lastColumnNumber = -1
        }
    }
    override def characters(ch: Array[Char], start: Int, length: Int): Unit = {
        if (vIsOpen) value.append(ch, start, length)
    }
    def nameToColumn(name: String): Int = {
        var column: Int = -1
        for (i <- 0 until name.length) {
            val c: Int = name.charAt(i)
            column = (column + 1) * 26 + c - 'A'
        }
        column
    }
}
class XlsxStreamReader (var fileName : String) {
    var xlsxPackage : OPCPackage = _
    var minColumns = -1
    var output = new ArrayList()
    var sharedStringsTable : ReadOnlySharedStringsTable = _
    def startReadProcess() : Unit = {
        val xlsxFile = new File(fileName)
        this.xlsxPackage = OPCPackage.open(xlsxFile.getPath(), PackageAccess.READ)
        println("*********Start Reading Process********")
        startParshingProcess()
    }
    def startParshingProcess() : Unit = {
        println("*********Start Parshing Process********")
        this.sharedStringsTable = new ReadOnlySharedStringsTable(this.xlsxPackage)
        val xssfReader = new XSSFReader(this.xlsxPackage)
        val styles = xssfReader.getStylesTable()
        val iter = xssfReader.getSheetsData().asInstanceOf[XSSFReader.SheetIterator]
        var index = 0
        breakable {
            while (iter.hasNext()) {
                var stream = iter.next()
                var sheetName = iter.getSheetName(); 
                println(sheetName + " [index=" + index + "]: is processing.")
                processSheet(styles, this.sharedStringsTable, stream)
                index += 1
                stream.close()
                break
            }
        }
    }
    def processSheet(styles: StylesTable, strings : ReadOnlySharedStringsTable, sheetInputStream:  InputStream) : Unit = {
        println("###########start process first sheet of xlsx#######")
        val sheetSource = new InputSource(sheetInputStream);
        val saxFactory = SAXParserFactory.newInstance();
        val saxParser = saxFactory.newSAXParser();
        val sheetParser = saxParser.getXMLReader();
        var output = new ArrayList[Any]();
        val handler = new StreamXSSFSheetHandler(styles, strings, this.minColumns, output);
        sheetParser.setContentHandler(handler);
        sheetParser.parse(sheetSource);
    }
}

class XlsxToDataFrame  {
    def startProcess() : Unit = {
        val streamReader = new XlsxStreamReader("/home/hduser/my_jars/StudentInfo.xlsx")
        streamReader.startReadProcess()
        val fs = headersName.map(f => StructField(f, StringType, true))
        val schema = new StructType(fs.toArray)
        val rdd = spark.sparkContext.makeRDD(rowDataRDD)
        val dataFrame = spark.createDataFrame(rdd,schema)
        dataFrame.printSchema
        dataFrame.show()
    }
    
}

val xlsxToDataFrame = new XlsxToDataFrame()
xlsxToDataFrame.startProcess()

