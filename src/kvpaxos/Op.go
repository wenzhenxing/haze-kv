package kvpaxos

/**
 * Interface of operation
 */
type Op interface {
	toString() string
	getToken() int64
	getResponse() interface{}
	setResponse(interface{})
}

type Op_T struct {
	ResChan chan interface{}
	Token   int64
}

type GetOp struct {
	Key string
	Op_T
}

func GetOp_new(k string, t int64) *GetOp {
	o := &GetOp{}
	o.Key = k
	o.ResChan = make(chan interface{})
	o.Token = t
	return o
}

func (this *GetOp) toString() string {
	return "GetOp key=" + this.Key + " token=" + int642string(this.Token)
}

func (this *GetOp) getToken() int64 {
	return this.Token
}

func (this *GetOp) getResponse() interface{} {
	return <-this.ResChan
}

func (this *GetOp) setResponse(v interface{}) {
	this.ResChan <- v
}

type PutOp struct {
	Key   string
	Value string
	Op_T
}

func PutOp_new(k string, v string, t int64) *PutOp {
	o := &PutOp{}
	o.Key = k
	o.Value = v
	o.Token = t
	o.ResChan = make(chan interface{})
	return o
}

func (this *PutOp) toString() string {
	return "PutOp key=" + this.Key + " value=" + this.Value + " token=" +
		int642string(this.Token)
}

func (this *PutOp) getToken() int64 {
	return this.Token
}

func (this *PutOp) getResponse() interface{} {
	return <-this.ResChan
}

func (this *PutOp) setResponse(v interface{}){
	this.ResChan<-v
}

type PutAppendOp struct {
	Key   string
	Value string
	Op_T
}

func PutAppendOp_new(k string, v string, t int64) *PutAppendOp {
	o := &PutAppendOp{}
	o.Key = k
	o.Value = v
	o.Token = t
	o.ResChan = make(chan interface{})
	return o
}

func (this *PutAppendOp) toString() string {
	return "PutAppendOp key=" + this.Key + " value=" + this.Value + " token=" +
		int642string(this.Token)
}

func (this *PutAppendOp) getToken() int64 {
	return this.Token
}

func (this *PutAppendOp) getResponse() interface{} {
	return <-this.ResChan
}

func (this *PutAppendOp) setResponse(v interface{}){
	this.ResChan<-v
}
