package isolation

type Level interface {
	String() string
}

type isolationSerializable struct{}

func (i isolationSerializable) String() string { return "Serializable" }

type isolationSnapshot struct{}

func (i isolationSnapshot) String() string { return "SnapshotIsolation" }

var Serializable = isolationSerializable{}
var Snapshot = isolationSnapshot{}
