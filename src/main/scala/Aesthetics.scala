
//This class is used for methods pertaining to printing and making prints neater
object Aesthetics{

    val borderLength = 56

    def printBorderVert(borderCount: Int){//prints a number of vertical borders
		    for(i<-1 to borderCount)
		    println("|                                                      |")
	    }

    def printBorderVert(borderStr: String){//prints a vertical border around a given string
		    val halfBorderLength = (borderLength - borderStr.length()) / 2
		    var newStr = "|"
			val additionalSpace = borderStr.length % 2
		    for(i<-2 to halfBorderLength) newStr += " "
		    newStr += borderStr
		    for(i<-2 to halfBorderLength) newStr += " "
			if(additionalSpace == 1) newStr+=" "
		    newStr+="|"
		    println(newStr)
	    }

    def printBorderHorz(borderCount: Int){//prints a given number of horzontal borders
		    for(i<-1 to borderCount)
		    println("--------------------------------------------------------")
	    }

    def printHeader(headStr:String){//prints a given string enclosed by borders
        printBorderHorz(1)
        printBorderVert(headStr)
        printBorderHorz(1)
    }
	
	def getDayCol(dayStr:String): String = {//used for printing routine views and aligning the borders around the days of the week
		val max = 12
		val diff = max - dayStr.length()
		var resStr = "| " + dayStr
		for(i<-1 to diff) resStr += " "
		resStr += "|"
		return resStr
	}
}