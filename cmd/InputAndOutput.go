package cmd

import (
	"NewPipeLine/structures"
	"bufio"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)
type StdinReader struct {   // Структура "настойчивая консольная читалка"
	sc *bufio.Scanner
}

func NewStdinReader()*StdinReader {
	return &StdinReader{
		sc : bufio.NewScanner(os.Stdin),
	}
}

func (sR *StdinReader) Read(needText, errText string)int{
	var val int
	var err error
	for{
		fmt.Println(needText)
		sR.sc.Scan()
		val, err = strconv.Atoi(sR.sc.Text())
		if err != nil{
			fmt.Println(errText)
			continue
		}
		break
	}
	return val
}

////////////////////////////////////////////////////////////////////////


type MyInput struct{   // Структура источник данных для конвейера! используящая читалку выше для записи
	readForOut chan int
	inputBuff *structures.TSafeSlice
	sR  *StdinReader
}

func NewInput()*MyInput {
	return  &MyInput{
		readForOut : make(chan int),
		inputBuff : structures.NewTSafeSlice(),
		sR : NewStdinReader(),
	}
}
func (mI *MyInput) Read(needText, errText string)int {  // метод для реализации условия из задания
														// читает значения в глобальные переменные и нетолько
	return mI.sR.Read(needText, errText)
}

func (mI *MyInput) collect(val int){
	mI.readForOut <- val
	mI.inputBuff.Add(val)
}

func (mI *MyInput) getChan() <-chan int {
	return mI.readForOut
}

func (mI *MyInput) GetInputBuff() *structures.TSafeSlice {
	return mI.inputBuff
}

func (mI *MyInput) Start(wg *sync.WaitGroup)<-chan int{


	var stopVal int  // значение остановки горутины опросника опросника

	stopVal = mI.Read("Введите значение остановки ввода", "Значение не распознано")

	go func(){
		defer wg.Done()
		for {
			num := mI.Read("Введите значение для обработки", "Значение не распознано")
			if num == stopVal{
				fmt.Println("the final value is entered, the program is ending")
				close(mI.readForOut)
				break
			}
			mI.collect(num)

		}
	}()
	return mI.getChan()
}




// Структура потребитель данных конвейера, разряжает буфер в консоль с настраиваемой периодичностью


type Output struct{
	buff structures.MyBuffer
	releaseBuffPer int
}

func NewOutPut(size int, releaseBuffPer int) *Output {
	if size < 1{
		size = 1
	}
	if releaseBuffPer < 3{
		releaseBuffPer = 3
	}
	return &Output{
		buff : structures.NewOldBuff(size),
		releaseBuffPer : releaseBuffPer,
	}
}

func (mO *Output) Start(ch <-chan int , slInput *structures.TSafeSlice){

	go func(){

		for val := range ch{
			mO.buff.Put(val)
		}
	}()
	go func(){
		for{
			time.Sleep(time.Second * time.Duration(mO.releaseBuffPer))
			res, numOfErr := mO.buff.Unload()

			fmt.Printf("Введено: %v за период  %v сек, получено : %v, не поместилось в буфер: %v\n",
				*slInput.Result() , mO.releaseBuffPer, *res, numOfErr)
			slInput.Clean()
		}
	}()
}

