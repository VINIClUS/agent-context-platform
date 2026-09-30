// Package store keeps things.
package store

import (
	"fmt"
	str "strings"
	. "math"
	_ "embed"
	"github.com/example/go-yaml"
	"gopkg.in/yaml.v3"
	"github.com/example/lib/v2"
)

import `os`

// Version is the schema version.
const Version = 3

const (
	Low, High = 1, 2
	Mid
)

var (
	ErrClosed = fmt.Errorf("closed")
	count     int
)

type Reader interface {
	Read(p []byte) (int, error)
	fmt.Stringer
	Closer
}

type Closer interface {
	Close() error
}

// Base is embedded below.
type Base struct{ id int }

type DB struct {
	Base
	*Cache
	fmt.Stringer
	name string `json:"name"`
}

type Cache map[string]int

type Alias = Cache

type Pair[K comparable, V any] struct {
	Key K
	Val V
}

func Open(name string) (*DB, error) {
	db := &DB{name: name}
	db.init()
	if err := check(name); err != nil {
		return nil, err
	}
	fmt.Println(str.ToUpper(name), Abs(1))
	_ = os.Getenv("HOME")
	_ = yaml.Marshal(nil)
	_ = lib.Do()
	return db, nil
}

func check(name string) error {
	unknown(name)
	return nil
}

func (db *DB) Close() error {
	db.flush()
	return nil
}

func (DB) flush() {}

func (db DB) init() {
	go func() { check(db.name) }()
}

func (p *Pair[K, V]) Swap() {}

func init() { count++ }

func init() { count-- }
