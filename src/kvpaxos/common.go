package kvpaxos

import "fmt"
import "strconv"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
	return
}

func ERROR(format string, a ...interface{}) {
	fmt.Printf(format, a)
}

func int642string(i int64)string{
	return strconv.FormatInt(i, 10)
}

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
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
