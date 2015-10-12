package redis

import (
	"strings"
)

//------------------------------------------------------------------------------
// XADD key [FINITY nums] [PRUNING minscore|maxscore] [ELEMENTS] score member [score member ...]
// XINCRBY key [FINITY nums] [PRUNING minscore|maxscore] [ELEMENTS] increment member

func (c *commandable) OnXADD(req *Request) Cmder {
	var (
		cmd      Cmder
		elements bool
	)
	elements = false
	for _, v := range req.cmd {
		if strings.ToUpper(v) == "ELEMENTS" {
			elements = true
			break
		}
	}
	if elements {
		cmd = NewSliceCmd(req.cmd...)
		c.Process(cmd)
		return cmd
	} else {
		cmd = NewIntCmd(req.cmd...)
		c.Process(cmd)
		return cmd
	}
}

// XINCRBY key [FINITY nums] [PRUNING minscore|maxscore] [ELEMENTS] increment member
func (c *commandable) OnXINCRBY(req *Request) Cmder {
	var (
		cmd      Cmder
		elements bool
	)
	elements = false
	for _, v := range req.cmd {
		if strings.ToUpper(v) == "ELEMENTS" {
			elements = true
			break
		}
	}
	if elements {
		cmd = NewSliceCmd(req.cmd...)
		c.Process(cmd)
		return cmd
	} else {
		cmd = NewFloatCmd(req.cmd...)
		c.Process(cmd)
		return cmd
	}
}

// XSETOPTIONS key [FINITY nums] [PRUNING minscore|maxscore] [ELEMENTS]
func (c *commandable) OnXSETOPTIONS(req *Request) Cmder {
	var (
		cmd      Cmder
		elements bool
	)
	elements = false
	for _, v := range req.cmd {
		if strings.ToUpper(v) == "ELEMENTS" {
			elements = true
			break
		}
	}
	if elements {
		cmd = NewSliceCmd(req.cmd...)
		c.Process(cmd)
		return cmd
	} else {
		cmd = NewIntCmd(req.cmd...)
		c.Process(cmd)
		return cmd
	}
}

func (c *commandable) OnXSCORE(req *Request) *FloatCmd {
	cmd := NewFloatCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnXCARD(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnXREM(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

// XGETFINITY key
func (c *commandable) OnXGETFINITY(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.process(cmd)
	return cmd
}

// XGETPRUNING key
func (c *commandable) OnXGETPRUNING(req *Request) *StringCmd {
	cmd := NewStringCmd(req.cmd...)
	c.process(cmd)
	return cmd
}

// XSETOPTIONS key [FINITY nums] [PRUNING minscore|maxscore] [ELEMENTS]
// func (c *commandable) XSetOptions(key string, finity string, pruning string, elements bool) *IntCmd {

// }

// XRANGE key start stop [withscores]
// 返回值 :
// 默认返回member。
// 当带withscores参数时，返回结果是member score。

// XREVRANGE key start stop [withscores]
// 返回值 : (倒序)
// 默认返回member。
// 当带withscores参数时，返回结果是member score。

func (c *commandable) OnXRANGE(req *Request) *StringSliceCmd {
	cmd := NewStringSliceCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnXREVRANGE(req *Request) *StringSliceCmd {
	cmd := NewStringSliceCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}
