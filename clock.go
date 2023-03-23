package deltago

import "time"

type Clock interface {
	NowInMillis() int64
	NowInNano() int64
	WaitTill(target int64) int64
}

type SystemClock struct{}

func (s *SystemClock) NowInMillis() int64 {
	return time.Now().UnixMilli()
}

func (s *SystemClock) NowInNano() int64 {
	return time.Now().UnixNano()
}

func (s *SystemClock) WaitTill(target int64) int64 {
	// time in millisecond
	dur := time.Duration(target - time.Now().UnixMilli())

	c := time.After(time.Millisecond * dur)
	cur := <-c
	return cur.UnixMilli()
}
