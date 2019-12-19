package replayer

import (
	LOG "github.com/vinllen/log4go"
	"gopkg.in/mgo.v2"
	conf "mongoshake/receiver/configure"
	"time"
)

var oneSession *mgo.Session

type SessionStore struct {
	session *mgo.Session
}

func InitMongoConnect()  {
	var err error

	dialInfo := &mgo.DialInfo{
		Addrs:          []string{conf.Options.DestMongoAddress},
		Username:		conf.Options.DestMongoUsername,
		Password:       conf.Options.DestMongoPassword,
		Database:       conf.Options.DestMongoDB,
		Direct:         false,
		Timeout:        time.Second * 3,
		FailFast:       false,
	}

	oneSession, err = mgo.DialWithInfo(dialInfo)

	if err != nil {
		LOG.Crash("Connect Mongo Address Failure %#v", err)
	}

	oneSession.SetSafe(&mgo.Safe{})
	oneSession.SetMode(mgo.Monotonic, true)
	oneSession.SetPoolLimit(20)

}

// 获取数据库的collection
func (ss * SessionStore) C(name string) *mgo.Collection {
	return ss.session.DB(conf.Options.DestMongoDB).C(name)
}

func NewSessionStore() *SessionStore  {
	ss := &SessionStore {
		session : oneSession.Copy(),
	}
	return ss
}

func (ss *SessionStore) Close()  {
	ss.session.Close()
}

func GetErrNotFound() error  {
	return mgo.ErrNotFound
}
