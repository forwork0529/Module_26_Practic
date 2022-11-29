package structures

import (
	"sync"
)



func DeMultiplexingFunc(dataSourceChan <-chan int, amount int) []chan int {
	var output = make([]chan int, amount)				   // слайс каналов

	for i := range output {
		output[i] = make(chan int)							// убедительно сделали канал каждом эксземляре выходного слайса каналов
	}
	go func() {												// внешняя горутина только для закрытия
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {											// внутртенняя горутина итерируется по выходному слайсу каналов
			defer wg.Done()
			i := 0
			for v := range dataSourceChan {					// итерируемся по датасорусу
				output[i] <- v								// пишем в один из каналов
				i++
				if i == amount{
					i = 0
				}
			}
		}()
		wg.Wait()

		for _, c := range output {
			close(c)
		}
	}()
	return output					// вернули результирующий слайс кналов
}


func MultiplexingFunc(channels ...chan int) <-chan int {

	var wg sync.WaitGroup

	multiplexedChan := make(chan int)
	multiplex := func(c <-chan int) {
		defer wg.Done()
		for i := range c {
			multiplexedChan <- i
		}
	}
	wg.Add(len(channels))
	for _, c := range channels {

		go multiplex(c)
	}

	go func() {
		wg.Wait()
		close(multiplexedChan)
	}()
	return multiplexedChan
}
