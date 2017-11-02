import java.io.File
import java.io.FileInputStream
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.xssf.streaming.SXSSFWorkbook
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.poi.ss.usermodel.WorkbookFactory
import org.apache.poi.ss.usermodel.Cell
import org.apache.poi.ss.usermodel.DataFormatter
import org.apache.poi.ss.usermodel.DateUtil
import scala.collection.JavaConversions._
import org.apache.poi.openxml4j.opc.OPCPackage
import org.apache.spark.sql.RowFactory
import java.util.ArrayList

import org.apache.spark.sql.types.{StructField, StringType}
import org.apache.spark.sql.types.StructType

final val fileName = "/home/hduser/my_jars/StudentInfo.xlsx"

val myFile = new File(fileName)

val pkg = OPCPackage.open(myFile);
val wb = new XSSFWorkbook(pkg);

val sheet=wb.getSheetAt(0);

Console.println(s"Sheet Name: ${sheet.getSheetName()}")
val rowIterator = sheet.rowIterator()
rowIterator.hasNext
val headers = rowIterator.next()
val fs = headers.map(f => StructField(f.getStringCellValue, StringType, true))
val schema = new StructType(fs.toArray)

val rowDataRDD = new ArrayList[org.apache.spark.sql.Row]()
while(rowIterator.hasNext){
val row = rowIterator.next()
val cellIterator = row.cellIterator()
val rowData = new ArrayList[Any]()
while(cellIterator.hasNext) {
val cell = cellIterator.next()
cell.getCellType match {
case Cell.CELL_TYPE_STRING => {
rowData.add(cell.getStringCellValue+"")
}
case Cell.CELL_TYPE_NUMERIC => {
rowData.add(cell.getNumericCellValue+"")
}
case Cell.CELL_TYPE_BOOLEAN => {
rowData.add(cell.getBooleanCellValue+"")
}
}
}
rowDataRDD.add(org.apache.spark.sql.Row.fromSeq(rowData))
}
val rdd = spark.sparkContext.makeRDD(rowDataRDD)
val dataFrame = spark.createDataFrame(rdd,schema)
dataFrame.printSchema
dataFrame.show