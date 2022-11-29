
package structures


import "sync"


// TSafe - Thread safe slice

type TSafeSlice struct{
	mu sync.Mutex
	sl []int
}

func NewTSafeSlice()*TSafeSlice {
	return &TSafeSlice{
		mu : sync.Mutex{},
		sl : make([]int, 0),
	}
}

func (t *TSafeSlice)  Add(val int){
	t.mu.Lock()
	t.sl = append(t.sl, val)
	t.mu.Unlock()
}

func (t *TSafeSlice) Clean(){
	t.mu.Lock()
	t.sl = make([]int, 0, 0)
	t.mu.Unlock()
}

func (t *TSafeSlice) Result() *[]int{

	res := make ([]int, len(t.sl), len(t.sl))
	t.mu.Lock()
	for i := 0; i < len(t.sl); i++ {
		res[i] = t.sl[i]
	}
	t.mu.Unlock()
	return &res
}
