package functions

type PipeLineFunc func(int)bool


// Стадия фильтрации отрицательных чисел (не пропускать отрицательные числа).

var NoMinus PipeLineFunc = func(val int)bool{
	if val > 0 {
		return true
	}
	return false
}

//Стадия фильтрации чисел, не кратных 3 (не пропускать такие числа), исключая также и 0.

var By3AndNo0 PipeLineFunc = func(val int)bool{
	if val != 0 && val % 3 == 0 {
		return true
	}
	return false
}

var ToDo []PipeLineFunc = make([]PipeLineFunc, 0, 2)

