package feature_logging

import(
	"log"
	"os"
)

// Конфигурирование логирования через переменные:
var loggerOn = true
var addLogToFile = false
var infoLog *log.Logger
var F *os.File

func LoggerStart() {

	if !loggerOn {
		return
	}
	if addLogToFile {
		var err error
		F, err = os.Open("log.txt")
		if err == nil{
			F.Close()
			os.Remove("log.txt")
		}
		F, err = os. Create("log.txt")
		if err != nil {
			panic(err)
		}
		infoLog = log.New(F, "INFO\t", log.Ldate|log.Ltime)
		return
	}
	infoLog = log.New(os.Stdout, "INFO\t", log.Ldate|log.Ltime)

}

func LoggerStop() {
	if loggerOn && addLogToFile {
		defer F.Close()
	}
}


func InfoLogging(mess string){
	if loggerOn {
		infoLog.Printf(mess)
	}

}



