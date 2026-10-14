package quota

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// This file is a minimal Redis client: RESP protocol plus a small connection
// pool. It exists so the gateway can share quota state across replicas without
// taking on a third-party dependency. Only the commands the store uses are
// implemented.

type redisValue struct {
	typ   byte
	str   string
	int   int64
	array []redisValue
	isNil bool
}

func (v redisValue) IsNil() bool   { return v.isNil }
func (v redisValue) IsError() bool { return v.typ == '-' }
func (v redisValue) Error() string { return strings.TrimPrefix(v.str, "ERR ") }
func (v redisValue) Text() string {
	switch v.typ {
	case '+', '$':
		return v.str
	case ':':
		return strconv.FormatInt(v.int, 10)
	default:
		return ""
	}
}
func (v redisValue) Int() int64 { return v.int }

var errProtocol = errors.New("redis protocol error")

type redisConn struct {
	net    net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
	read   time.Duration
	write  time.Duration
}

func dialRedis(cfg redisConfig) (*redisConn, error) {
	conn, err := net.DialTimeout("tcp", cfg.addr, cfg.dialTimeout)
	if err != nil {
		return nil, err
	}
	c := &redisConn{
		net:    conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
		read:   cfg.readTimeout,
		write:  cfg.writeTimeout,
	}
	if err := c.bootstrap(cfg); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return c, nil
}

func (c *redisConn) bootstrap(cfg redisConfig) error {
	if cfg.password != "" {
		if _, err := c.do("AUTH", cfg.password); err != nil {
			return err
		}
	}
	if cfg.db > 0 {
		if _, err := c.do("SELECT", strconv.Itoa(cfg.db)); err != nil {
			return err
		}
	}
	return nil
}

func (c *redisConn) do(args ...string) (redisValue, error) {
	if err := c.writeCommand(args); err != nil {
		return redisValue{}, err
	}
	return c.readReply()
}

func (c *redisConn) writeCommand(args []string) error {
	if c.write > 0 {
		if err := c.net.SetWriteDeadline(time.Now().Add(c.write)); err != nil {
			return err
		}
	}
	if _, err := c.writer.WriteString("*" + strconv.Itoa(len(args)) + "\r\n"); err != nil {
		return err
	}
	for _, arg := range args {
		if _, err := c.writer.WriteString("$" + strconv.Itoa(len(arg)) + "\r\n" + arg + "\r\n"); err != nil {
			return err
		}
	}
	return c.writer.Flush()
}

func (c *redisConn) readReply() (redisValue, error) {
	if c.read > 0 {
		if err := c.net.SetReadDeadline(time.Now().Add(c.read)); err != nil {
			return redisValue{}, err
		}
	}
	line, err := c.reader.ReadString('\n')
	if err != nil {
		return redisValue{}, err
	}
	if len(line) < 3 {
		return redisValue{}, errProtocol
	}
	body := strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	switch typ := body[0]; typ {
	case '+':
		return redisValue{typ: typ, str: body[1:]}, nil
	case '-':
		return redisValue{typ: typ, str: body[1:]}, nil
	case ':':
		n, err := strconv.ParseInt(body[1:], 10, 64)
		if err != nil {
			return redisValue{}, errProtocol
		}
		return redisValue{typ: typ, int: n}, nil
	case '$':
		n, err := strconv.Atoi(body[1:])
		if err != nil {
			return redisValue{}, errProtocol
		}
		if n < 0 {
			return redisValue{typ: typ, isNil: true}, nil
		}
		payload := make([]byte, n+2)
		if _, err := readFull(c.reader, payload); err != nil {
			return redisValue{}, err
		}
		return redisValue{typ: typ, str: string(payload[:n])}, nil
	case '*':
		n, err := strconv.Atoi(body[1:])
		if err != nil {
			return redisValue{}, errProtocol
		}
		if n < 0 {
			return redisValue{typ: typ, isNil: true}, nil
		}
		values := make([]redisValue, 0, n)
		for i := 0; i < n; i++ {
			value, err := c.readReply()
			if err != nil {
				return redisValue{}, err
			}
			values = append(values, value)
		}
		return redisValue{typ: typ, array: values}, nil
	default:
		return redisValue{}, errProtocol
	}
}

func readFull(reader *bufio.Reader, buffer []byte) (int, error) {
	total := 0
	for total < len(buffer) {
		n, err := reader.Read(buffer[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

type redisConfig struct {
	addr         string
	password     string
	db           int
	poolSize     int
	dialTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration
}

type redisPool struct {
	cfg   redisConfig
	conns chan *redisConn
	once  sync.Once
}

func newRedisPool(cfg redisConfig) *redisPool {
	if cfg.poolSize <= 0 {
		cfg.poolSize = 8
	}
	if cfg.dialTimeout <= 0 {
		cfg.dialTimeout = 2 * time.Second
	}
	if cfg.readTimeout <= 0 {
		cfg.readTimeout = 2 * time.Second
	}
	if cfg.writeTimeout <= 0 {
		cfg.writeTimeout = 2 * time.Second
	}
	return &redisPool{cfg: cfg, conns: make(chan *redisConn, cfg.poolSize)}
}

func (p *redisPool) Close() error {
	p.once.Do(func() {
		close(p.conns)
		for conn := range p.conns {
			_ = conn.net.Close()
		}
	})
	return nil
}

// run borrows a connection, executes one command and returns it to the pool. A
// broken connection is dropped and the command is retried once on a fresh one.
func (p *redisPool) run(args []string) (redisValue, error) {
	for attempt := 0; attempt < 2; attempt++ {
		conn, err := p.borrow()
		if err != nil {
			return redisValue{}, err
		}
		value, err := conn.do(args...)
		if err != nil {
			_ = conn.net.Close()
			continue
		}
		p.release(conn)
		if value.IsError() {
			return value, errors.New(value.Error())
		}
		return value, nil
	}
	return redisValue{}, ErrUnavailable
}

func (p *redisPool) borrow() (*redisConn, error) {
	select {
	case conn := <-p.conns:
		return conn, nil
	default:
	}
	return dialRedis(p.cfg)
}

func (p *redisPool) release(conn *redisConn) {
	select {
	case p.conns <- conn:
	default:
		_ = conn.net.Close()
	}
}

// evalScript runs a script by its SHA1, falling back to EVAL so the very first
// call against a fresh Redis still works.
func (p *redisPool) evalScript(source string, numKeys int, keys, args []string) (redisValue, error) {
	sha := scriptSHA(source)
	full := make([]string, 0, len(keys)+len(args)+3)
	full = append(full, "EVALSHA", sha, strconv.Itoa(numKeys))
	full = append(full, keys...)
	full = append(full, args...)

	value, err := p.run(full)
	if err != nil && strings.Contains(strings.ToUpper(err.Error()), "NOSCRIPT") {
		boot := make([]string, 0, len(keys)+len(args)+3)
		boot = append(boot, "EVAL", source, strconv.Itoa(numKeys))
		boot = append(boot, keys...)
		boot = append(boot, args...)
		return p.run(boot)
	}
	return value, err
}

func scriptSHA(source string) string {
	sum := sha1.Sum([]byte(source))
	return hex.EncodeToString(sum[:])
}

// Ping verifies connectivity, used at startup to fail loudly rather than later.
func (p *redisPool) Ping() error {
	value, err := p.run([]string{"PING"})
	if err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}
	if strings.ToUpper(value.Text()) != "PONG" {
		return fmt.Errorf("redis ping returned %q", value.Text())
	}
	return nil
}
