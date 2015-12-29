package pbservice

const (
	OK                 = "OK"
	ErrNoKey           = "ErrNoKey"
	ErrWrongServer     = "ErrWrongServer"
	ErrCopyNotFinished = "ErrCopyNotFinished"
	ErrDuplicated      = "ErrDuplicated"
	ErrSync            = "Sync to buckup error"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	Op     string
	From   string
	Rpc_id int //the kv-server filter all Num>0

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	From   string
	Rpc_id int
}

type GetReply struct {
	Err   Err
	Value string
}

type CopyArgs struct {
	Data     map[string]string
	From     string
	Last_rpc map[string]int
	Rpc_id   int
}

type CopyReply struct {
	Err Err
}

// Your RPC definitions here.
