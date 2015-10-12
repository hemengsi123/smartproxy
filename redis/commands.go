package redis

import (
	"io"
	"strconv"
	// "strings"
	"time"

	log "github.com/ngaut/logging"
)

func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func formatInt(i int64) string {
	return strconv.FormatInt(i, 10)
}

func readTimeout(timeout time.Duration) time.Duration {
	if timeout == 0 {
		return 0
	}
	return timeout + time.Second
}

type commandable struct {
	process func(cmd Cmder)
}

func (c *commandable) Process(cmd Cmder) {
	c.process(cmd)
}

func usePrecise(dur time.Duration) bool {
	return dur < time.Second || dur%time.Second != 0
}

func formatMs(dur time.Duration) string {
	if dur > 0 && dur < time.Millisecond {
		log.Warningf(
			"redis: specified duration is %s, but minimal supported value is %s",
			dur, time.Millisecond,
		)
	}
	return strconv.FormatInt(int64(dur/time.Millisecond), 10)
}

func formatSec(dur time.Duration) string {
	if dur > 0 && dur < time.Second {
		log.Warningf(
			"redis: specified duration is %s, but minimal supported value is %s",
			dur, time.Second,
		)
	}
	return strconv.FormatInt(int64(dur/time.Second), 10)
}

//------------------------------------------------------------------------------

func (c *commandable) OnUnDenfined(req *Request) *StringCmd {

	// args := req.Args()
	cmd := NewStringCmd("Meet " + req.Name())

	cmd.err = UnDefinedErr
	return cmd
}

func (c *commandable) OnReflectUnvalid(req *Request) *StringCmd {

	// args := req.Args()
	cmd := NewStringCmd("Meet " + req.Name())

	cmd.err = ReflectUnvalidErr
	return cmd
}

//------------------------------------------------------------------------------

func (c *commandable) Auth(password string) *StatusCmd {
	cmd := newKeylessStatusCmd("AUTH", password)
	c.Process(cmd)
	return cmd
}

func (c *commandable) Echo(message string) *StringCmd {
	cmd := NewStringCmd("ECHO", message)
	cmd._clusterKeyPos = 0
	c.Process(cmd)
	return cmd
}

func (c *commandable) Ping() *StatusCmd {
	cmd := newKeylessStatusCmd("PING")
	c.Process(cmd)
	return cmd
}

func (c *commandable) Quit() *StatusCmd {
	log.Fatal("not implemented")
	return nil
}

func (c *commandable) Select(index int64) *StatusCmd {
	cmd := newKeylessStatusCmd("SELECT", strconv.FormatInt(index, 10))
	c.Process(cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *commandable) OnDEL(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnDUMP(req *Request) *StringCmd {
	cmd := NewStringCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnEXISTS(req *Request) *BoolCmd {
	cmd := NewBoolCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnEXPIRE(req *Request) *BoolCmd {
	cmd := NewBoolCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnEXPIREAT(req *Request) *BoolCmd {
	cmd := NewBoolCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) Keys(pattern string) *StringSliceCmd {
	cmd := NewStringSliceCmd("KEYS", pattern)
	c.Process(cmd)
	return cmd
}

func (c *commandable) Migrate(host, port, key string, db int64, timeout time.Duration) *StatusCmd {
	cmd := NewStatusCmd(
		"MIGRATE",
		host,
		port,
		key,
		strconv.FormatInt(db, 10),
		formatMs(timeout),
	)
	cmd._clusterKeyPos = 3
	cmd.setReadTimeout(readTimeout(timeout))
	c.Process(cmd)
	return cmd
}

func (c *commandable) Move(key string, db int64) *BoolCmd {
	cmd := NewBoolCmd("MOVE", key, strconv.FormatInt(db, 10))
	c.Process(cmd)
	return cmd
}

func (c *commandable) ObjectRefCount(keys ...string) *IntCmd {
	args := append([]string{"OBJECT", "REFCOUNT"}, keys...)
	cmd := NewIntCmd(args...)
	cmd._clusterKeyPos = 2
	c.Process(cmd)
	return cmd
}

func (c *commandable) ObjectEncoding(keys ...string) *StringCmd {
	args := append([]string{"OBJECT", "ENCODING"}, keys...)
	cmd := NewStringCmd(args...)
	cmd._clusterKeyPos = 2
	c.Process(cmd)
	return cmd
}

func (c *commandable) ObjectIdleTime(keys ...string) *DurationCmd {
	args := append([]string{"OBJECT", "IDLETIME"}, keys...)
	cmd := NewDurationCmd(time.Second, args...)
	cmd._clusterKeyPos = 2
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnPERSIST(req *Request) *BoolCmd {
	cmd := NewBoolCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnPEXPIRE(req *Request) *BoolCmd {
	cmd := NewBoolCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnPEXPIREAT(req *Request) *BoolCmd {
	cmd := NewBoolCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnPTTL(req *Request) *DurationCmd {
	cmd := NewDurationCmd(time.Millisecond, req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) RandomKey() *StringCmd {
	cmd := NewStringCmd("RANDOMKEY")
	c.Process(cmd)
	return cmd
}

func (c *commandable) Rename(key, newkey string) *StatusCmd {
	cmd := NewStatusCmd("RENAME", key, newkey)
	c.Process(cmd)
	return cmd
}

func (c *commandable) RenameNX(key, newkey string) *BoolCmd {
	cmd := NewBoolCmd("RENAMENX", key, newkey)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnRESTORE(req *Request) *StatusCmd {
	cmd := NewStatusCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

type Sort struct {
	By            string
	Offset, Count float64
	Get           []string
	Order         string
	IsAlpha       bool
	Store         string
}

// func (c *commandable) Sort(key string, sort Sort) *StringSliceCmd {
// 	args := []string{"SORT", key}
// 	if sort.By != "" {
// 		args = append(args, "BY", sort.By)
// 	}
// 	if sort.Offset != 0 || sort.Count != 0 {
// 		args = append(args, "LIMIT", formatFloat(sort.Offset), formatFloat(sort.Count))
// 	}
// 	for _, get := range sort.Get {
// 		args = append(args, "GET", get)
// 	}
// 	if sort.Order != "" {
// 		args = append(args, sort.Order)
// 	}
// 	if sort.IsAlpha {
// 		args = append(args, "ALPHA")
// 	}
// 	if sort.Store != "" {
// 		args = append(args, "STORE", sort.Store)
// 	}
// 	cmd := NewStringSliceCmd(args...)
// 	c.Process(cmd)
// 	return cmd
// }

func (c *commandable) OnTTL(req *Request) *DurationCmd {
	cmd := NewDurationCmd(time.Second, req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnTYPE(req *Request) *StatusCmd {
	cmd := NewStatusCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

// func (c *commandable) Scan(cursor int64, match string, count int64) *ScanCmd {
// 	args := []string{"SCAN", strconv.FormatInt(cursor, 10)}
// 	if match != "" {
// 		args = append(args, "MATCH", match)
// 	}
// 	if count > 0 {
// 		args = append(args, "COUNT", strconv.FormatInt(count, 10))
// 	}
// 	cmd := NewScanCmd(args...)
// 	c.Process(cmd)
// 	return cmd
// }

func (c *commandable) SScan(key string, cursor int64, match string, count int64) *ScanCmd {
	args := []string{"SSCAN", key, strconv.FormatInt(cursor, 10)}
	if match != "" {
		args = append(args, "MATCH", match)
	}
	if count > 0 {
		args = append(args, "COUNT", strconv.FormatInt(count, 10))
	}
	cmd := NewScanCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) HScan(key string, cursor int64, match string, count int64) *ScanCmd {
	args := []string{"HSCAN", key, strconv.FormatInt(cursor, 10)}
	if match != "" {
		args = append(args, "MATCH", match)
	}
	if count > 0 {
		args = append(args, "COUNT", strconv.FormatInt(count, 10))
	}
	cmd := NewScanCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ZScan(key string, cursor int64, match string, count int64) *ScanCmd {
	args := []string{"ZSCAN", key, strconv.FormatInt(cursor, 10)}
	if match != "" {
		args = append(args, "MATCH", match)
	}
	if count > 0 {
		args = append(args, "COUNT", strconv.FormatInt(count, 10))
	}
	cmd := NewScanCmd(args...)
	c.Process(cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *commandable) OnAPPEND(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

type BitCount struct {
	Start, End int64
}

func (c *commandable) OnBITCOUNT(req *Request) *IntCmd {
	// args := []string{"BITCOUNT", key}
	// if bitCount != nil {
	// 	args = append(
	// 		args,
	// 		strconv.FormatInt(bitCount.Start, 10),
	// 		strconv.FormatInt(bitCount.End, 10),
	// 	)
	// }
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) bitOp(op, destKey string, keys ...string) *IntCmd {
	args := []string{"BITOP", op, destKey}
	args = append(args, keys...)
	cmd := NewIntCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) BitOpAnd(destKey string, keys ...string) *IntCmd {
	return c.bitOp("AND", destKey, keys...)
}

func (c *commandable) BitOpOr(destKey string, keys ...string) *IntCmd {
	return c.bitOp("OR", destKey, keys...)
}

func (c *commandable) BitOpXor(destKey string, keys ...string) *IntCmd {
	return c.bitOp("XOR", destKey, keys...)
}

func (c *commandable) BitOpNot(destKey string, key string) *IntCmd {
	return c.bitOp("NOT", destKey, key)
}

func (c *commandable) BitPos(key string, bit int64, pos ...int64) *IntCmd {
	args := []string{"BITPOS", key, formatInt(bit)}
	switch len(pos) {
	case 0:
	case 1:
		args = append(args, formatInt(pos[0]))
	case 2:
		args = append(args, formatInt(pos[0]), formatInt(pos[1]))
	default:
		panic("too many arguments")
	}
	cmd := NewIntCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnDECR(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnDECRBY(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnGET(req *Request) *StringCmd {
	var cmd *StringCmd
	args := req.Args()

	if len(args) != 1 {
		cmd = NewStringCmd("GET", "")
	} else {
		cmd = NewStringCmd("GET", args[0])
	}
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnGETBIT(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnGETRANGE(req *Request) *StringCmd {
	cmd := NewStringCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnGETSET(req *Request) *StringCmd {
	cmd := NewStringCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnINCR(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnINCRBY(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnINCRBYFLOAT(req *Request) *FloatCmd {
	cmd := NewFloatCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) MGet(keys ...string) *SliceCmd {
	args := append([]string{"MGET"}, keys...)
	cmd := NewSliceCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) MSet(pairs ...string) *StatusCmd {
	args := append([]string{"MSET"}, pairs...)
	cmd := NewStatusCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) MSetNX(pairs ...string) *BoolCmd {
	args := append([]string{"MSETNX"}, pairs...)
	cmd := NewBoolCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnSET(req *Request) *StatusCmd {
	// args := []string{"SET", key, value}
	// if expiration > 0 {
	// 	if usePrecise(expiration) {
	// 		args = append(args, "PX", formatMs(expiration))
	// 	} else {
	// 		args = append(args, "EX", formatSec(expiration))
	// 	}
	// }
	// cmd := NewStatusCmd(args...)
	// c.Process(cmd)
	// return cmd
	// var cmd *StatusCmd
	// args := req.Args()
	// NewStatusCmd(...)
	// if len(args) != 1 {
	// 	cmd = NewStringCmd("GET", "")
	// } else {
	// 	cmd = NewStringCmd("GET", args[0])
	// }
	cmd := NewStatusCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnPSETEX(req *Request) *StatusCmd {
	cmd := NewStatusCmd(req.cmd...)
	c.process(cmd)
	return cmd
}

func (c *commandable) OnSETBIT(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnSETNX(req *Request) *BoolCmd {
	cmd := NewBoolCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnSETEX(req *Request) *StatusCmd {
	// cmd := NewBoolCmd(req.cmd...)
	cmd := NewStatusCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *Client) OnSETXX(req *Request) *BoolCmd {
	cmd := NewBoolCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnSETRANGE(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnSTRLEN(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *commandable) OnHDEL(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnHEXISTS(req *Request) *BoolCmd {
	cmd := NewBoolCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnHGET(req *Request) *StringCmd {
	cmd := NewStringCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnHGETALL(req *Request) *StringSliceCmd {
	cmd := NewStringSliceCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

// func (c *commandable) HGetAllMap(key string) *StringStringMapCmd {
// 	cmd := NewStringStringMapCmd("HGETALL", key)
// 	c.Process(cmd)
// 	return cmd
// }

func (c *commandable) OnHINCRBY(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnHINCRBYFLOAT(req *Request) *FloatCmd {
	cmd := NewFloatCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnHKEYS(req *Request) *StringSliceCmd {
	cmd := NewStringSliceCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnHLEN(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnHMGET(req *Request) *SliceCmd {
	cmd := NewSliceCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnHMSET(req *Request) *StatusCmd {
	cmd := NewStatusCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnHSET(req *Request) *BoolCmd {
	cmd := NewBoolCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnHSETNX(req *Request) *BoolCmd {
	cmd := NewBoolCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnHVALS(req *Request) *StringSliceCmd {
	cmd := NewStringSliceCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *commandable) BLPop(timeout time.Duration, keys ...string) *StringSliceCmd {
	args := append([]string{"BLPOP"}, keys...)
	args = append(args, formatSec(timeout))
	cmd := NewStringSliceCmd(args...)
	cmd.setReadTimeout(readTimeout(timeout))
	c.Process(cmd)
	return cmd
}

func (c *commandable) BRPop(timeout time.Duration, keys ...string) *StringSliceCmd {
	args := append([]string{"BRPOP"}, keys...)
	args = append(args, formatSec(timeout))
	cmd := NewStringSliceCmd(args...)
	cmd.setReadTimeout(readTimeout(timeout))
	c.Process(cmd)
	return cmd
}

func (c *commandable) BRPopLPush(source, destination string, timeout time.Duration) *StringCmd {
	cmd := NewStringCmd(
		"BRPOPLPUSH",
		source,
		destination,
		formatSec(timeout),
	)
	cmd.setReadTimeout(readTimeout(timeout))
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnLINDEX(req *Request) *StringCmd {
	cmd := NewStringCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnLINSERT(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnLLEN(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnLPOP(req *Request) *StringCmd {
	cmd := NewStringCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnLPUSH(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnLPUSHX(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnLRANGE(req *Request) *StringSliceCmd {
	cmd := NewStringSliceCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnLREM(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnLSET(req *Request) *StatusCmd {
	cmd := NewStatusCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnLTRIM(req *Request) *StatusCmd {
	cmd := NewStatusCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnRPOP(req *Request) *StringCmd {
	cmd := NewStringCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnRPOPLPUSH(req *Request) *StringCmd {
	cmd := NewStringCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnRPUSH(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnRPUSHX(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *commandable) OnSADD(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnSCARD(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

// func (c *commandable) SDiff(keys ...string) *StringSliceCmd {
// 	args := append([]string{"SDIFF"}, keys...)
// 	cmd := NewStringSliceCmd(args...)
// 	c.Process(cmd)
// 	return cmd
// }

func (c *commandable) SDiffStore(destination string, keys ...string) *IntCmd {
	args := append([]string{"SDIFFSTORE", destination}, keys...)
	cmd := NewIntCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) SInter(keys ...string) *StringSliceCmd {
	args := append([]string{"SINTER"}, keys...)
	cmd := NewStringSliceCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) SInterStore(destination string, keys ...string) *IntCmd {
	args := append([]string{"SINTERSTORE", destination}, keys...)
	cmd := NewIntCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnSISMEMBER(req *Request) *BoolCmd {
	cmd := NewBoolCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnSMEMBERS(req *Request) *StringSliceCmd {
	cmd := NewStringSliceCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

// func (c *commandable) SMove(source, destination, member string) *BoolCmd {
// 	cmd := NewBoolCmd("SMOVE", source, destination, member)
// 	c.Process(cmd)
// 	return cmd
// }

func (c *commandable) OnSPOP(req *Request) *StringCmd {
	cmd := NewStringCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnSRANDMEMBER(req *Request) *StringCmd {
	cmd := NewStringCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnSREM(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

// func (c *commandable) SUnion(keys ...string) *StringSliceCmd {
// 	args := append([]string{"SUNION"}, keys...)
// 	cmd := NewStringSliceCmd(args...)
// 	c.Process(cmd)
// 	return cmd
// }

// func (c *commandable) SUnionStore(destination string, keys ...string) *IntCmd {
// 	args := append([]string{"SUNIONSTORE", destination}, keys...)
// 	cmd := NewIntCmd(args...)
// 	c.Process(cmd)
// 	return cmd
// }

type Z struct {
	Score  float64
	Member string
}

type ZStore struct {
	Weights   []int64
	Aggregate string
}

func (c *commandable) OnZADD(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnZCARD(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnZCOUNT(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnZINCRBY(req *Request) *FloatCmd {
	cmd := NewFloatCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ZInterStore(
	destination string,
	store ZStore,
	keys ...string,
) *IntCmd {
	args := []string{"ZINTERSTORE", destination, strconv.FormatInt(int64(len(keys)), 10)}
	args = append(args, keys...)
	if len(store.Weights) > 0 {
		args = append(args, "WEIGHTS")
		for _, weight := range store.Weights {
			args = append(args, strconv.FormatInt(weight, 10))
		}
	}
	if store.Aggregate != "" {
		args = append(args, "AGGREGATE", store.Aggregate)
	}
	cmd := NewIntCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) zRange(key string, start, stop int64, withScores bool) *StringSliceCmd {
	args := []string{
		"ZRANGE",
		key,
		strconv.FormatInt(start, 10),
		strconv.FormatInt(stop, 10),
	}
	if withScores {
		args = append(args, "WITHSCORES")
	}
	cmd := NewStringSliceCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnZRANGE(req *Request) *StringSliceCmd {
	cmd := NewStringSliceCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ZRangeWithScores(key string, start, stop int64) *ZSliceCmd {
	args := []string{
		"ZRANGE",
		key,
		strconv.FormatInt(start, 10),
		strconv.FormatInt(stop, 10),
		"WITHSCORES",
	}
	cmd := NewZSliceCmd(args...)
	c.Process(cmd)
	return cmd
}

type ZRangeByScore struct {
	Min, Max      string
	Offset, Count int64
}

func (c *commandable) zRangeByScore(key string, opt ZRangeByScore, withScores bool) *StringSliceCmd {
	args := []string{"ZRANGEBYSCORE", key, opt.Min, opt.Max}
	if withScores {
		args = append(args, "WITHSCORES")
	}
	if opt.Offset != 0 || opt.Count != 0 {
		args = append(
			args,
			"LIMIT",
			strconv.FormatInt(opt.Offset, 10),
			strconv.FormatInt(opt.Count, 10),
		)
	}
	cmd := NewStringSliceCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnZRANGEBYSCORE(req *Request) *StringSliceCmd {
	cmd := NewStringSliceCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnZRANGEBYLEX(req *Request) *StringSliceCmd {
	cmd := NewStringSliceCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnZLEXCOUNT(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnZREMRANGEBYLEX(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ZRangeByScoreWithScores(key string, opt ZRangeByScore) *ZSliceCmd {
	args := []string{"ZRANGEBYSCORE", key, opt.Min, opt.Max, "WITHSCORES"}
	if opt.Offset != 0 || opt.Count != 0 {
		args = append(
			args,
			"LIMIT",
			strconv.FormatInt(opt.Offset, 10),
			strconv.FormatInt(opt.Count, 10),
		)
	}
	cmd := NewZSliceCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnZRANK(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnZREM(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnZREMRANGEBYRANK(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnZREMRANGEBYSCORE(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnZREVRANGE(req *Request) *StringSliceCmd {
	cmd := NewStringSliceCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

// func (c *commandable) ZRevRangeWithScores(key string, start, stop int64) *ZSliceCmd {
// 	cmd := NewZSliceCmd("ZREVRANGE", key, formatInt(start), formatInt(stop), "WITHSCORES")
// 	c.Process(cmd)
// 	return cmd
// }

func (c *commandable) OnZREVRANGEBYSCORE(req *Request) *StringSliceCmd {
	cmd := NewStringSliceCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ZRevRangeByScoreWithScores(key string, opt ZRangeByScore) *ZSliceCmd {
	args := []string{"ZREVRANGEBYSCORE", key, opt.Max, opt.Min, "WITHSCORES"}
	if opt.Offset != 0 || opt.Count != 0 {
		args = append(
			args,
			"LIMIT",
			strconv.FormatInt(opt.Offset, 10),
			strconv.FormatInt(opt.Count, 10),
		)
	}
	cmd := NewZSliceCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnZREVRANK(req *Request) *IntCmd {
	cmd := NewIntCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) OnZSCORE(req *Request) *FloatCmd {
	cmd := NewFloatCmd(req.cmd...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ZUnionStore(dest string, store ZStore, keys ...string) *IntCmd {
	args := []string{"ZUNIONSTORE", dest, strconv.FormatInt(int64(len(keys)), 10)}
	args = append(args, keys...)
	if len(store.Weights) > 0 {
		args = append(args, "WEIGHTS")
		for _, weight := range store.Weights {
			args = append(args, strconv.FormatInt(weight, 10))
		}
	}
	if store.Aggregate != "" {
		args = append(args, "AGGREGATE", store.Aggregate)
	}
	cmd := NewIntCmd(args...)
	c.Process(cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *commandable) BgRewriteAOF() *StatusCmd {
	cmd := NewStatusCmd("BGREWRITEAOF")
	cmd._clusterKeyPos = 0
	c.Process(cmd)
	return cmd
}

func (c *commandable) BgSave() *StatusCmd {
	cmd := NewStatusCmd("BGSAVE")
	cmd._clusterKeyPos = 0
	c.Process(cmd)
	return cmd
}

func (c *commandable) ClientKill(ipPort string) *StatusCmd {
	cmd := NewStatusCmd("CLIENT", "KILL", ipPort)
	cmd._clusterKeyPos = 0
	c.Process(cmd)
	return cmd
}

func (c *commandable) ClientList() *StringCmd {
	cmd := NewStringCmd("CLIENT", "LIST")
	cmd._clusterKeyPos = 0
	c.Process(cmd)
	return cmd
}

func (c *commandable) ClientPause(dur time.Duration) *BoolCmd {
	cmd := NewBoolCmd("CLIENT", "PAUSE", formatMs(dur))
	cmd._clusterKeyPos = 0
	c.Process(cmd)
	return cmd
}

func (c *commandable) ConfigGet(parameter string) *SliceCmd {
	cmd := NewSliceCmd("CONFIG", "GET", parameter)
	cmd._clusterKeyPos = 0
	c.Process(cmd)
	return cmd
}

func (c *commandable) ConfigResetStat() *StatusCmd {
	cmd := NewStatusCmd("CONFIG", "RESETSTAT")
	cmd._clusterKeyPos = 0
	c.Process(cmd)
	return cmd
}

func (c *commandable) ConfigSet(parameter, value string) *StatusCmd {
	cmd := NewStatusCmd("CONFIG", "SET", parameter, value)
	cmd._clusterKeyPos = 0
	c.Process(cmd)
	return cmd
}

func (c *commandable) DbSize() *IntCmd {
	cmd := NewIntCmd("DBSIZE")
	cmd._clusterKeyPos = 0
	c.Process(cmd)
	return cmd
}

func (c *commandable) FlushAll() *StatusCmd {
	cmd := newKeylessStatusCmd("FLUSHALL")
	c.Process(cmd)
	return cmd
}

func (c *commandable) FlushDb() *StatusCmd {
	cmd := newKeylessStatusCmd("FLUSHDB")
	c.Process(cmd)
	return cmd
}

func (c *commandable) Info() *StringCmd {
	cmd := NewStringCmd("INFO")
	cmd._clusterKeyPos = 0
	c.Process(cmd)
	return cmd
}

func (c *commandable) LastSave() *IntCmd {
	cmd := NewIntCmd("LASTSAVE")
	cmd._clusterKeyPos = 0
	c.Process(cmd)
	return cmd
}

func (c *commandable) Save() *StatusCmd {
	cmd := newKeylessStatusCmd("SAVE")
	c.Process(cmd)
	return cmd
}

func (c *commandable) shutdown(modifier string) *StatusCmd {
	var args []string
	if modifier == "" {
		args = []string{"SHUTDOWN"}
	} else {
		args = []string{"SHUTDOWN", modifier}
	}
	cmd := newKeylessStatusCmd(args...)
	c.Process(cmd)
	if err := cmd.Err(); err != nil {
		if err == io.EOF {
			// Server quit as expected.
			cmd.err = nil
		}
	} else {
		// Server did not quit. String reply contains the reason.
		cmd.err = errorf(cmd.val)
		cmd.val = ""
	}
	return cmd
}

func (c *commandable) Shutdown() *StatusCmd {
	return c.shutdown("")
}

func (c *commandable) ShutdownSave() *StatusCmd {
	return c.shutdown("SAVE")
}

func (c *commandable) ShutdownNoSave() *StatusCmd {
	return c.shutdown("NOSAVE")
}

func (c *commandable) SlaveOf(host, port string) *StatusCmd {
	cmd := newKeylessStatusCmd("SLAVEOF", host, port)
	c.Process(cmd)
	return cmd
}

func (c *commandable) SlowLog() {
	panic("not implemented")
}

func (c *commandable) Sync() {
	panic("not implemented")
}

func (c *commandable) Time() *StringSliceCmd {
	cmd := NewStringSliceCmd("TIME")
	cmd._clusterKeyPos = 0
	c.Process(cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *commandable) Eval(script string, keys []string, args []string) *Cmd {
	cmdArgs := []string{"EVAL", script, strconv.FormatInt(int64(len(keys)), 10)}
	cmdArgs = append(cmdArgs, keys...)
	cmdArgs = append(cmdArgs, args...)
	cmd := NewCmd(cmdArgs...)
	if len(keys) > 0 {
		cmd._clusterKeyPos = 3
	}
	c.Process(cmd)
	return cmd
}

func (c *commandable) EvalSha(sha1 string, keys []string, args []string) *Cmd {
	cmdArgs := []string{"EVALSHA", sha1, strconv.FormatInt(int64(len(keys)), 10)}
	cmdArgs = append(cmdArgs, keys...)
	cmdArgs = append(cmdArgs, args...)
	cmd := NewCmd(cmdArgs...)
	if len(keys) > 0 {
		cmd._clusterKeyPos = 3
	}
	c.Process(cmd)
	return cmd
}

func (c *commandable) ScriptExists(scripts ...string) *BoolSliceCmd {
	args := append([]string{"SCRIPT", "EXISTS"}, scripts...)
	cmd := NewBoolSliceCmd(args...)
	cmd._clusterKeyPos = 0
	c.Process(cmd)
	return cmd
}

func (c *commandable) ScriptFlush() *StatusCmd {
	cmd := newKeylessStatusCmd("SCRIPT", "FLUSH")
	c.Process(cmd)
	return cmd
}

func (c *commandable) ScriptKill() *StatusCmd {
	cmd := newKeylessStatusCmd("SCRIPT", "KILL")
	c.Process(cmd)
	return cmd
}

func (c *commandable) ScriptLoad(script string) *StringCmd {
	cmd := NewStringCmd("SCRIPT", "LOAD", script)
	cmd._clusterKeyPos = 0
	c.Process(cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *commandable) DebugObject(key string) *StringCmd {
	cmd := NewStringCmd("DEBUG", "OBJECT", key)
	cmd._clusterKeyPos = 2
	c.Process(cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *commandable) PubSubChannels(pattern string) *StringSliceCmd {
	args := []string{"PUBSUB", "CHANNELS"}
	if pattern != "*" {
		args = append(args, pattern)
	}
	cmd := NewStringSliceCmd(args...)
	cmd._clusterKeyPos = 0
	c.Process(cmd)
	return cmd
}

func (c *commandable) PubSubNumSub(channels ...string) *StringIntMapCmd {
	args := []string{"PUBSUB", "NUMSUB"}
	args = append(args, channels...)
	cmd := NewStringIntMapCmd(args...)
	cmd._clusterKeyPos = 0
	c.Process(cmd)
	return cmd
}

func (c *commandable) PubSubNumPat() *IntCmd {
	cmd := NewIntCmd("PUBSUB", "NUMPAT")
	cmd._clusterKeyPos = 0
	c.Process(cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *commandable) ClusterSlots() *ClusterSlotCmd {
	cmd := NewClusterSlotCmd("CLUSTER", "slots")
	cmd._clusterKeyPos = 0
	c.Process(cmd)
	return cmd
}

func (c *commandable) ClusterNodes() *StringCmd {
	cmd := NewStringCmd("CLUSTER", "nodes")
	cmd._clusterKeyPos = 0
	c.Process(cmd)
	return cmd
}

func (c *commandable) ClusterMeet(host, port string) *StatusCmd {
	cmd := newKeylessStatusCmd("CLUSTER", "meet", host, port)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ClusterReplicate(nodeID string) *StatusCmd {
	cmd := newKeylessStatusCmd("CLUSTER", "replicate", nodeID)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ClusterInfo() *StringCmd {
	cmd := NewStringCmd("CLUSTER", "info")
	cmd._clusterKeyPos = 0
	c.Process(cmd)
	return cmd
}

func (c *commandable) ClusterFailover() *StatusCmd {
	cmd := newKeylessStatusCmd("CLUSTER", "failover")
	c.Process(cmd)
	return cmd
}

func (c *commandable) ClusterAddSlots(slots ...int) *StatusCmd {
	args := make([]string, len(slots)+2)
	args[0] = "CLUSTER"
	args[1] = "addslots"
	for i, num := range slots {
		args[i+2] = strconv.Itoa(num)
	}
	cmd := newKeylessStatusCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ClusterAddSlotsRange(min, max int) *StatusCmd {
	size := max - min + 1
	slots := make([]int, size)
	for i := 0; i < size; i++ {
		slots[i] = min + i
	}
	return c.ClusterAddSlots(slots...)
}
