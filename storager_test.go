package storage

import (
	"log"
	"testing"
)

func TestStorage(t *testing.T) {
	s, err := GetStorager("t.db", "t1")
	if err != nil {
		t.Error(err.Error())
	}

	s.AddOrUpdate("h1", []byte("h1"))
	s.Close()
	s2, err := GetStorager("t.db", "t2")
	if err != nil {
		t.Error(err.Error())
	}

	log.Println(s2.GetAll())
	s2.Close()

	s, err = GetStorager("t.db", "t1")
	if err != nil {
		t.Error(err.Error())
	}

	s.AddOrUpdate("h2", []byte("h2"))

	s.AddOrUpdate("h3", []byte("h3"))

	log.Println(s2.GetAll())

	log.Println("------------")
	log.Println(s.Get("h1"))

	log.Println(s.GetAll())

}
