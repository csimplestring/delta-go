package op

import "github.com/samber/mo"

// we do not support metrics here

type Operation struct {
	Name           Name
	Parameters     map[string]string
	UserParameters mo.Option[map[string]string]
	UserMetadata   mo.Option[string]
}
