package storage

import (
	"errors"
	"itranslater/pkg/logger"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/boltdb/bolt"
)

// BoltStorage 使用Bolt实现的存储
type BoltStorage struct {
	db         *bolt.DB
	bucketName string
	contents   sync.Map
	count      int64
}

// NewBoltStorage will return a boltdb object and error.
func NewBoltStorage(fileName string, bucketName string) (*BoltStorage, error) {

	if fileName == "" {
		return nil, errors.New("open boltdb whose fileName is empty")
	}

	if bucketName == "" {
		return nil, errors.New("create a bucket whose name is empty")
	}

	db, err := bolt.Open(fileName, 0600, nil)
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	storage := &BoltStorage{
		db:         db,
		bucketName: bucketName,
	}

	// Sync data from database to memory.
	storage.loadData()
	logger.Tracef("db(%s)初始化完毕.", fileName)
	return storage, nil
}

// Exist will check the given key is existed in DB or not.
func (s *BoltStorage) Exist(key string) bool {
	_, ok := s.contents.Load(key)
	return ok
}

// Get will get the value of key.
func (s *BoltStorage) Get(key string) []byte {
	var value []byte
	if temp, ok := s.contents.Load(key); ok {

		if content, ok := temp.([]byte); ok {
			value = append(value, content...)
		}
	}

	return value
}

// Delete the value by the given key.
func (s *BoltStorage) Delete(key string) bool {
	isSucceed := false
	err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(s.bucketName)).Delete([]byte(key))
	})

	if err == nil {
		isSucceed = true
		if _, ok := s.contents.Load(key); ok {
			s.contents.Delete(key)
			atomic.AddInt64(&s.count, -1)
		}
	}

	return isSucceed
}

// AddOrUpdate 新增或更新键值
func (s *BoltStorage) AddOrUpdate(key string, value []byte) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(s.bucketName)).Put([]byte(key), value)
	})

	if err == nil {
		if _, loaded := s.contents.LoadOrStore(key, value); !loaded {
			atomic.AddInt64(&s.count, 1)
		}
	}

	return err
}

// GetAll 获取所有数据
func (s *BoltStorage) GetAll() map[string][]byte {
	result := make(map[string][]byte)

	s.contents.Range(func(key, value interface{}) bool {
		if k, ok := key.(string); ok {
			if v, ok := value.([]byte); ok {
				result[k] = v
			}
		}

		return true
	})

	return result
}

// Close 关闭数据库连接
func (s *BoltStorage) Close() {
	s.db.Close()
}

// loadData 加载数据到内存
func (s *BoltStorage) loadData() {
	s.db.View(func(tx *bolt.Tx) error {
		tx.Bucket([]byte(s.bucketName)).ForEach(func(k, v []byte) error {
			key, value := make([]byte, len(k)), make([]byte, len(v))
			copy(key, k)
			copy(value, v)
			//logger.Error(string(key))
			s.contents.Store(string(key), value)
			atomic.AddInt64(&s.count, 1)
			return nil
		})

		return nil
	})
}

// GetRandomOne Get one random record.
func (s *BoltStorage) GetRandomKey() string {

	if s.count == 0 {
		return ""
	}

	var randomKey string
	var defaultKey string
	index := rand.New(rand.NewSource(time.Now().Unix())).Intn(int(atomic.LoadInt64(&s.count)))

	s.contents.Range(func(key, value interface{}) bool {
		// Set a default key to avoid that other goroutine is deleting content at the same time.
		if defaultKey == "" {
			defaultKey, _ = key.(string)
		}

		if index == 0 {
			randomKey, _ = key.(string)
			return false
		}

		index--
		return true
	})

	if randomKey == "" {
		randomKey = defaultKey
	}

	return randomKey
}

// Len 获取存储数量
func (s *BoltStorage) Len() int64 {
	return s.count
}
