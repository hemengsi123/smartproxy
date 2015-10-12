package redis

import (
	"strings"
)

type Request struct {
	cmd   []string
	reply []byte
	err   error
	resp  Cmder
}

func (r *Request) Name() string {
	if len(r.cmd) > 0 {
		return strings.ToUpper(r.cmd[0])
	}
	return ""
}

func (r *Request) Len() int {
	return len(r.cmd)
}

func (r *Request) Args() []string {
	if len(r.cmd) > 0 {
		return r.cmd[1:]
	}
	return []string{}
}

func (r *Request) StringAtIndex(i int) string {
	if i >= r.Len() {
		return ""
	}
	return string(r.cmd[i])
}

func (r *Request) Result() []byte {
	if r.err != nil {
		return []byte("-" + r.err.Error() + "\r\n")
	}
	return r.reply
}

func (r *Request) SetReply(d []byte) {
	r.reply = d
}

func (r *Request) SetError(e error) {
	r.err = e
}

func (r *Request) SetResp(cmd Cmder) {
	r.resp = cmd
	r.SetReply(cmd.Reply())
}

func NewRequest(req []string) *Request {
	return &Request{
		cmd: req,
	}
}
