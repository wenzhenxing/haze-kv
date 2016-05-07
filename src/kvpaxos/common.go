package kvpaxos

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

const (
	OP_GET    = "GET"
	OP_APPEND = "APPEND"
	OP_PUT    = "PUT"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	Token int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key   string
	Token int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
