spark-shell --jars /home/hduser/my_jars/poi-3.9/commons-codec-1.5.jar,/home/hduser/my_jars/poi-3.9/commons-logging-1.1.jar,/home/hduser/my_jars/poi-3.9/dom4j-1.6.1.jar,/home/hduser/my_jars/poi-3.9/junit-3.8.1.jar,/home/hduser/my_jars/poi-3.9/log4j-1.2.13.jar,/home/hduser/my_jars/poi-3.9/poi-3.9-20121203.jar,/home/hduser/my_jars/poi-3.9/poi-examples-3.9-20121203.jar,/home/hduser/my_jars/poi-3.9/poi-excelant-3.9-20121203.jar,/home/hduser/my_jars/poi-3.9/poi-ooxml-3.9-20121203.jar,/home/hduser/my_jars/poi-3.9/poi-ooxml-schemas-3.9-20121203.jar,/home/hduser/my_jars/poi-3.9/poi-scratchpad-3.9-20121203.jar,/home/hduser/my_jars/poi-3.9/stax-api-1.0.1.jar,/home/hduser/my_jars/poi-3.9/xmlbeans-2.3.0.jar

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

import org.apache.spark.sql.types.{StructField, StringType}

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
val schema = new StructType()
for(elem <- fs) schema.add(elem)

rowIterator.hasNext
val rowData = rowIterator.next()

while(rowIterator.hasNext){
     
          val row = rowIterator.next()
     
            val cellIterator = row.cellIterator()
            while(cellIterator.hasNext) {
              val cell = cellIterator.next()
                cell.getCellType match {
                  case Cell.CELL_TYPE_STRING => {
                    print(cell.getStringCellValue + "\t")
                  }
                  case Cell.CELL_TYPE_NUMERIC => {
                    print(cell.getNumericCellValue + "\t")
                  }
                  case Cell.CELL_TYPE_BOOLEAN => {
                    print(cell.getBooleanCellValue + "\t")
                  }
                  case Cell.CELL_TYPE_BLANK => {
                    print("null" + "\t")
                  }
                  case _ => throw new RuntimeException(" this error occured when reading ")
                  //        case Cell.CELL_TYPE_FORMULA => {print(cell.getF + "\t")}
                }
            }
            println("")
      }


      import org.apache.spark.sql.types.{StructField, StringType}
      val fs = headers.split(",").map(f => StructField(f, StringType))