package watcher

const (
	remoteChangeListSize = 100
	localChangeListSize  = 200
)

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
