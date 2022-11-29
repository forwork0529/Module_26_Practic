package structures

import (
	"errors"
	"strconv"
	"sync"
)

type MyBuffer interface{
	Put(int)error
	Get()(int, error)
	Unload()(*[]string, int)

}


var buffIsEmpty error = errors.New("buffer is empty")
var buffOverLoaded error = errors.New("buffer overloaded")


// Пораздумав решил что при перегрузке буфер может отбрасывать получаемые значения выдавая ошибку,
// получив которую при работе необходимо увеличивать мощность буфера..

// Буффер, первая версия на основе массива без использования каналов ----------------------------

type BuffOld struct {
	head   int
	tail   int
	curLen int
	absLen int
	muD    sync.Mutex
	data   []int
	errors int
}

func NewOldBuff(len int) *BuffOld {
	return &BuffOld{absLen : len, data : make([]int, len), muD: sync.Mutex{}}
}

func (b *BuffOld) Put(val int) error{
	defer b.muD.Unlock()
	b.muD.Lock()
	if b.curLen + 1 > b.absLen{
		b.errors += 1
		return buffOverLoaded
	}
	b.data[b.tail] = val
	b.tail = (b.tail + 1) % b.absLen
	b.curLen += 1
	return nil
}

func (b *BuffOld) Get() (int , error){
	if b.curLen == 0{
		return 0, buffIsEmpty
	}
	b.muD.Lock()
	res := b.data[b.head]
	b.head = (b.head + 1) % b.absLen
	b.curLen -= 1
	b.muD.Unlock()
	return res, nil
}

func (b *BuffOld) Unload() (*[]string, int){
	res := make([]string, b.absLen)
	for i := 0; i < b.absLen; i ++{
		val, err := b.Get()
		if err != nil{
			res[i] = "nil"
			continue
		}
		res[i] = strconv.Itoa(val)
	}
	errors := b.errors
	b.muD.Lock()
	b.errors = 0
	b.muD.Unlock()
	return &res, errors
}





