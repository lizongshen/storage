package storage

// Storager 存储接口
type Storager interface {
	Exist(string) bool
	Get(string) []byte
	Delete(string) bool
	AddOrUpdate(string, []byte) error
	GetAll() map[string][]byte
	Len() int64
	Close()
	GetRandomKey() string
}

// GetStorager 获取存储接口
func GetStorager(fileName string, bucketName string) (Storager, error) {
	return NewBoltStorage(fileName, bucketName)
}
