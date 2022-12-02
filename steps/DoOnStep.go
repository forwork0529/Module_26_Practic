package steps

import (
	fl "NewPipeLine/feature_logging"
	fu "NewPipeLine/functions"
	"NewPipeLine/structures"
	"fmt"
)


// структура шаг
// принимает функцию и количество каналов исполнения


type Step struct{
	f func(int)bool
	nGo int
}

func NewStep(f fu.PipeLineFunc, nGo int) *Step {
	return &Step{
		f : f,
		nGo : nGo,
	}
}


func (s *Step) Start2(ch <-chan int)<-chan int{

	splitChannels := structures.DeMultiplexingFunc(ch, s.nGo) // получил много каналов из одного входного

	toMuxChannels := make([]chan int, s.nGo) // Заготовил пул канлов на мультиплексор, в кторые пишет функция
	for i := range toMuxChannels{
		toMuxChannels[i] = make(chan int)
	}


	for i := 0; i < s.nGo; i++ {            // Запустили горутины обрабатывающие каналы после демультиплексации
		go func(chIN, chOUT chan int  ) {
			defer close(chOUT)
			for val := range chIN{
				fl.InfoLogging(fmt.Sprintf("Step, step %v got value: %v ", s.f, val))
				if s.f(val){
					fl.InfoLogging(fmt.Sprintf("Step, step %v value %v accepted", s.f, val))
					chOUT <- val
					continue
				}
				fl.InfoLogging(fmt.Sprintf("Step, step %v value %v discarded", s.f, val))
			}
		}(splitChannels[i], toMuxChannels[i])
	}

	toOutputChan := structures.MultiplexingFunc(toMuxChannels...)
	fl.InfoLogging("Step of PipeLine, prepared a pull of channels")
	return toOutputChan
}

type pipeLine struct{
	chIn <-chan int
	funcs []fu.PipeLineFunc
}

func NewPipeLine(chIn <-chan int, funcs []fu.PipeLineFunc)*pipeLine {   // Модуль конвеер получает входной канал и список функций
	return &pipeLine{
		chIn : chIn,
		funcs : funcs,
	}
}

func (pl *pipeLine) Start(nGo int)<-chan int{							// Стартуя конвеер просит количество потоков обработки

	if nGo < 1{
		nGo = 1
	}

	var prevStepChan <-chan int
	if len(pl.funcs) == 0{												// Если список функций пуст: пуляем со входа сразу на выход
		prevStepChan = pl.chIn
	}
	for i, fun := range pl.funcs{										// Итерируясь по списку функций:
		step := NewStep(fun, nGo)										// создаём шаги, каждому шагу вручаем по функции
		if i == 0 {
			prevStepChan = step.Start2(pl.chIn)							// если это первый шаг- берём канал из структуры
			continue
		}
		prevStepChan = step.Start2(prevStepChan)							// Выходной канал шага сохраняем как предидущий

	}
	fl.InfoLogging("PipeLine as build order ")
	return prevStepChan
}
