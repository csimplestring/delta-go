package action

import (
	"github.com/samber/mo"
)

func UtilFnCollect[R Action](actions []Action) []R {
	var res []R
	for _, action := range actions {
		v, ok := action.(R)
		if ok {
			res = append(res, v)
		}
	}
	return res
}

func UtilFnCollectFirst[R Action](actions []Action) mo.Option[R] {
	for _, a := range actions {
		v, ok := a.(R)
		if ok {
			return mo.Some(v)
		}
	}
	return mo.None[R]()
}

func UtilFnMapToString(actions []Action) ([]string, error) {
	strs := make([]string, len(actions))
	for i, a := range actions {
		str, err := a.Json()
		if err != nil {
			return nil, err
		}
		strs[i] = str
	}
	return strs, nil
}
