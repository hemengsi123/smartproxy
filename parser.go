package smartproxy

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

var (
	errReaderTooSmall = errors.New("redis: reader is too small")

	// [43 79 75 13 10]
	OK_BYTES = []byte("+OK\r\n")
	OK_PONG  = []byte("+PONG\r\n")
)

//------------------------------------------------------------------------------

func appendArgs(buf []byte, args []string) []byte {
	buf = append(buf, '*')
	buf = strconv.AppendUint(buf, uint64(len(args)), 10)
	buf = append(buf, '\r', '\n')
	for _, arg := range args {
		buf = append(buf, '$')
		buf = strconv.AppendUint(buf, uint64(len(arg)), 10)
		buf = append(buf, '\r', '\n')
		buf = append(buf, arg...)
		buf = append(buf, '\r', '\n')
	}
	return buf
}

//------------------------------------------------------------------------------

func readLine(rd *bufio.Reader) ([]byte, error) {
	// line, isPrefix, err := rd.ReadLine()
	// if err != nil {
	// 	return line, err
	// }
	// if isPrefix {
	// 	return line, errReaderTooSmall
	// }
	// if len(line) < 2 || line[len(line)-2] != '\r' { // \r\n
	// 	fmt.Println(line)
	// 	return nil, errors.New(fmt.Sprintf("invalid redis packet %v, err:%v", line, err))
	// }
	// return line, nil

	line, err := rd.ReadSlice('\n')
	if err != nil {
		return nil, err
	}

	if len(line) < 2 || line[len(line)-2] != '\r' { // \r\n
		return nil, errors.New(fmt.Sprintf("invalid redis packet %v, err:%v", line, err))
	}

	return line[:len(line)-2], nil
}

func readN(rd *bufio.Reader, n int) ([]byte, error) {
	// b, err := rd.ReadN(n)
	b := make([]byte, n)

	_, err := io.ReadFull(rd, b)
	if err == bufio.ErrBufferFull {
		tmp := make([]byte, n)
		r := copy(tmp, b)
		b = tmp

		for {
			nn, err := rd.Read(b[r:])
			r += nn
			if r >= n {
				// Ignore error if we read enough.
				break
			}
			if err != nil {
				return nil, err
			}
		}
	} else if err != nil {
		return nil, err
	}
	return b, nil
}

//------------------------------------------------------------------------------

func parseReq(rd *bufio.Reader) ([]string, error) {
	line, err := readLine(rd)
	if err != nil {
		return nil, err
	}

	if line[0] != '*' {
		return nil, errors.New(fmt.Sprintf("bad command %s ", string(line)))
	}
	numReplies, err := strconv.ParseInt(string(line[1:]), 10, 64)
	if err != nil {
		return nil, err
	}

	args := make([]string, 0, numReplies)
	for i := int64(0); i < numReplies; i++ {
		line, err = readLine(rd)
		if err != nil {
			return nil, err
		}
		if line[0] != '$' {
			return nil, fmt.Errorf("redis: expected '$', but got %q", line)
		}

		argLen, err := strconv.ParseInt(string(line[1:]), 10, 32)
		if err != nil {
			return nil, err
		}

		arg, err := readN(rd, int(argLen)+2)
		if err != nil {
			return nil, err
		}
		args = append(args, string(arg[:argLen]))
	}
	return args, nil
}
