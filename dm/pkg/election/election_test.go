// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package election

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/tikv/pd/pkg/utils/tempurl"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

var _ = check.SerialSuites(&testElectionSuite{})

func TestSuite(t *testing.T) {
	check.TestingT(t)
}

type testElectionSuite struct {
	etcd     *embed.Etcd
	endPoint string

	notifyBlockTime time.Duration
}

func (t *testElectionSuite) SetUpTest(c *check.C) {
	c.Assert(log.InitLogger(&log.Config{}), check.IsNil)

	cfg := embed.NewConfig()
	cfg.Name = "election-test"
	cfg.Dir = c.MkDir()
	cfg.ZapLoggerBuilder = embed.NewZapCoreLoggerBuilder(log.L().Logger, log.L().Core(), log.Props().Syncer)
	cfg.Logger = "zap"
	err := cfg.Validate() // verify & trigger the builder
	c.Assert(err, check.IsNil)

	t.endPoint = tempurl.Alloc()
	url2, err := url.Parse(t.endPoint)
	c.Assert(err, check.IsNil)
	cfg.ListenClientUrls = []url.URL{*url2}
	cfg.AdvertiseClientUrls = cfg.ListenClientUrls

	url2, err = url.Parse(tempurl.Alloc())
	c.Assert(err, check.IsNil)
	cfg.ListenPeerUrls = []url.URL{*url2}
	cfg.AdvertisePeerUrls = cfg.ListenPeerUrls

	cfg.InitialCluster = fmt.Sprintf("%s=%s", cfg.Name, url2)
	cfg.ClusterState = embed.ClusterStateFlagNew

	t.etcd, err = embed.StartEtcd(cfg)
	c.Assert(err, check.IsNil)
	select {
	case <-t.etcd.Server.ReadyNotify():
	case <-time.After(10 * time.Second):
		c.Fatal("start embed etcd timeout")
	}

	// some notify leader information is not handled, just reduce the block time and ignore them
	t.notifyBlockTime = 100 * time.Millisecond
}

func (t *testElectionSuite) TearDownTest(c *check.C) {
	t.etcd.Close()
}

func testElection2After1(t *testElectionSuite, c *check.C, normalExit bool) {
	var (
		timeout    = 3 * time.Second
		sessionTTL = 60
		key        = "unit-test/election-2-after-1"
		ID1        = "member1"
		ID2        = "member2"
		ID3        = "member3"
		addr1      = "127.0.0.1:1"
		addr2      = "127.0.0.1:2"
		addr3      = "127.0.0.1:3"
	)
	cli, err := etcdutil.CreateClient([]string{t.endPoint}, nil)
	c.Assert(err, check.IsNil)
	defer cli.Close()
	ctx0, cancel0 := context.WithCancel(context.Background())
	defer cancel0()
	_, err = cli.Delete(ctx0, key, clientv3.WithPrefix())
	c.Assert(err, check.IsNil)

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	if !normalExit {
		c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/pkg/election/mockCampaignLoopExitedAbnormally", `return()`), check.IsNil)
		//nolint:errcheck
		defer failpoint.Disable("github.com/pingcap/tiflow/dm/pkg/election/mockCampaignLoopExitedAbnormally")
	}
	e1, err := NewElection(ctx1, cli, sessionTTL, key, ID1, addr1, t.notifyBlockTime)
	c.Assert(err, check.IsNil)
	defer e1.Close()

	// e1 should become the leader
	select {
	case leader := <-e1.LeaderNotify():
		c.Assert(leader.ID, check.Equals, ID1)
	case <-time.After(timeout):
		c.Fatal("leader campaign timeout")
	}
	c.Assert(e1.IsLeader(), check.IsTrue)
	_, leaderID, leaderAddr, err := e1.LeaderInfo(ctx1)
	c.Assert(err, check.IsNil)
	c.Assert(leaderID, check.Equals, e1.ID())
	c.Assert(leaderAddr, check.Equals, addr1)
	if !normalExit {
		c.Assert(failpoint.Disable("github.com/pingcap/tiflow/dm/pkg/election/mockCampaignLoopExitedAbnormally"), check.IsNil)
	}

	// start e2
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	e2, err := NewElection(ctx2, cli, sessionTTL, key, ID2, addr2, t.notifyBlockTime)
	c.Assert(err, check.IsNil)
	defer e2.Close()
	select {
	case leader := <-e2.leaderCh:
		c.Assert(leader.ID, check.Equals, ID1)
	case <-time.After(timeout):
		c.Fatal("leader campaign timeout")
	}
	// but the leader should still be e1
	_, leaderID, leaderAddr, err = e2.LeaderInfo(ctx2)
	c.Assert(err, check.IsNil)
	c.Assert(leaderID, check.Equals, e1.ID())
	c.Assert(leaderAddr, check.Equals, addr1)
	c.Assert(e2.IsLeader(), check.IsFalse)

	var wg sync.WaitGroup
	e1.Close() // stop the campaign for e1
	c.Assert(e1.IsLeader(), check.IsFalse)

	ctx3, cancel3 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel3()
	deleted, err := e2.ClearSessionIfNeeded(ctx3, ID1)
	c.Assert(err, check.IsNil)
	if normalExit {
		// for normally exited election, session has already been closed before
		c.Assert(deleted, check.IsFalse)
	} else {
		// for abnormally exited election, session will be cleared here
		c.Assert(deleted, check.IsTrue)
	}

	// e2 should become the leader
	select {
	case leader := <-e2.LeaderNotify():
		c.Assert(leader.ID, check.Equals, ID2)
	case <-time.After(timeout):
		c.Fatal("leader campaign timeout")
	}
	c.Assert(e2.IsLeader(), check.IsTrue)
	_, leaderID, leaderAddr, err = e2.LeaderInfo(ctx2)
	c.Assert(err, check.IsNil)
	c.Assert(leaderID, check.Equals, e2.ID())
	c.Assert(leaderAddr, check.Equals, addr2)

	// only e2's election info is left in etcd
	ctx4, cancel4 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel4()
	resp, err := cli.Get(ctx4, key, clientv3.WithPrefix())
	c.Assert(err, check.IsNil)
	c.Assert(resp.Kvs, check.HasLen, 1)

	// if closing the client when campaigning, we should get an error
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case err2 := <-e2.ErrorNotify():
			c.Assert(terror.ErrElectionCampaignFail.Equal(err2), check.IsTrue)
			// the old session is done, but we can't create a new one.
			c.Assert(err2, check.ErrorMatches, ".*fail to campaign leader: create a new session.*")
		case <-time.After(timeout):
			c.Fatal("do not receive error for e2")
		}
	}()
	cli.Close() // close the client
	wg.Wait()

	// can not elect with closed client.
	ctx5, cancel5 := context.WithCancel(context.Background())
	defer cancel5()
	_, err = NewElection(ctx5, cli, sessionTTL, key, ID3, addr3, t.notifyBlockTime)
	c.Assert(terror.ErrElectionCampaignFail.Equal(err), check.IsTrue)
	c.Assert(err, check.ErrorMatches, ".*Message: fail to campaign leader: create the initial session, RawCause: context canceled.*")
	cancel0()
}

func (t *testElectionSuite) TestElection2After1(c *check.C) {
	testElection2After1(t, c, true)
	testElection2After1(t, c, false)
}

func (t *testElectionSuite) TestElectionAlways1(c *check.C) {
	var (
		timeout    = 3 * time.Second
		sessionTTL = 60
		key        = "unit-test/election-always-1"
		ID1        = "member1"
		ID2        = "member2"
		addr1      = "127.0.0.1:1234"
		addr2      = "127.0.0.1:2345"
	)
	cli, err := etcdutil.CreateClient([]string{t.endPoint}, nil)
	c.Assert(err, check.IsNil)
	defer cli.Close()

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	e1, err := NewElection(ctx1, cli, sessionTTL, key, ID1, addr1, t.notifyBlockTime)
	c.Assert(err, check.IsNil)
	defer e1.Close()

	// e1 should become the leader
	select {
	case leader := <-e1.LeaderNotify():
		c.Assert(leader.ID, check.Equals, ID1)
	case <-time.After(timeout):
		c.Fatal("leader campaign timeout")
	}
	c.Assert(e1.IsLeader(), check.IsTrue)
	_, leaderID, leaderAddr, err := e1.LeaderInfo(ctx1)
	c.Assert(err, check.IsNil)
	c.Assert(leaderID, check.Equals, e1.ID())
	c.Assert(leaderAddr, check.Equals, addr1)

	// start e2
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	e2, err := NewElection(ctx2, cli, sessionTTL, key, ID2, addr2, t.notifyBlockTime)
	c.Assert(err, check.IsNil)
	defer e2.Close()
	time.Sleep(100 * time.Millisecond) // wait 100ms to start the campaign
	// but the leader should still be e1
	_, leaderID, leaderAddr, err = e2.LeaderInfo(ctx2)
	c.Assert(err, check.IsNil)
	c.Assert(leaderID, check.Equals, e1.ID())
	c.Assert(leaderAddr, check.Equals, addr1)
	c.Assert(e2.IsLeader(), check.IsFalse)

	// cancel the campaign for e2, should get no errors
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case err2 := <-e2.ErrorNotify():
			c.Fatalf("cancel the campaign should not get an error, %v", err2)
		case <-time.After(timeout): // wait 3s
		}
	}()
	cancel2()
	wg.Wait()

	// e1 is still the leader
	c.Assert(e1.IsLeader(), check.IsTrue)
	_, leaderID, leaderAddr, err = e1.LeaderInfo(ctx1)
	c.Assert(err, check.IsNil)
	c.Assert(leaderID, check.Equals, e1.ID())
	c.Assert(leaderAddr, check.Equals, addr1)
	c.Assert(e2.IsLeader(), check.IsFalse)
}

func (t *testElectionSuite) TestElectionEvictLeader(c *check.C) {
	var (
		timeout    = 3 * time.Second
		sessionTTL = 60
		key        = "unit-test/election-evict-leader"
		ID1        = "member1"
		ID2        = "member2"
		addr1      = "127.0.0.1:1234"
		addr2      = "127.0.0.1:2345"
	)
	cli, err := etcdutil.CreateClient([]string{t.endPoint}, nil)
	c.Assert(err, check.IsNil)
	defer cli.Close()

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	e1, err := NewElection(ctx1, cli, sessionTTL, key, ID1, addr1, t.notifyBlockTime)
	c.Assert(err, check.IsNil)
	defer e1.Close()

	// e1 should become the leader
	select {
	case leader := <-e1.LeaderNotify():
		c.Assert(leader.ID, check.Equals, ID1)
	case <-time.After(timeout):
		c.Fatal("leader campaign timeout")
	}
	c.Assert(e1.IsLeader(), check.IsTrue)
	_, leaderID, leaderAddr, err := e1.LeaderInfo(ctx1)
	c.Assert(err, check.IsNil)
	c.Assert(leaderID, check.Equals, e1.ID())
	c.Assert(leaderAddr, check.Equals, addr1)

	// start e2
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	e2, err := NewElection(ctx2, cli, sessionTTL, key, ID2, addr2, t.notifyBlockTime)
	c.Assert(err, check.IsNil)
	defer e2.Close()
	time.Sleep(100 * time.Millisecond) // wait 100ms to start the campaign
	// but the leader should still be e1
	_, leaderID, leaderAddr, err = e2.LeaderInfo(ctx2)
	c.Assert(err, check.IsNil)
	c.Assert(leaderID, check.Equals, e1.ID())
	c.Assert(leaderAddr, check.Equals, addr1)
	c.Assert(e2.IsLeader(), check.IsFalse)

	// e1 evict leader, and e2 will be the leader
	e1.EvictLeader()
	utils.WaitSomething(8, 250*time.Millisecond, func() bool {
		_, leaderID, _, _ = e2.LeaderInfo(ctx2)
		return leaderID == e2.ID()
	})
	_, leaderID, leaderAddr, err = e2.LeaderInfo(ctx2)
	c.Assert(err, check.IsNil)
	c.Assert(leaderID, check.Equals, e2.ID())
	c.Assert(leaderAddr, check.Equals, addr2)
	utils.WaitSomething(10, 10*time.Millisecond, func() bool {
		return e2.IsLeader()
	})

	// cancel evict of e1, and then evict e2, e1 will be the leader
	e1.CancelEvictLeader()
	e2.EvictLeader()
	utils.WaitSomething(8, 250*time.Millisecond, func() bool {
		_, leaderID, _, _ = e1.LeaderInfo(ctx1)
		return leaderID == e1.ID()
	})
	_, leaderID, leaderAddr, err = e1.LeaderInfo(ctx1)
	c.Assert(err, check.IsNil)
	c.Assert(leaderID, check.Equals, e1.ID())
	c.Assert(leaderAddr, check.Equals, addr1)
	utils.WaitSomething(10, 10*time.Millisecond, func() bool {
		return e1.IsLeader()
	})
}

func (t *testElectionSuite) TestElectionDeleteKey(c *check.C) {
	var (
		timeout    = 3 * time.Second
		sessionTTL = 60
		key        = "unit-test/election-delete-key"
		ID         = "member"
		addr       = "127.0.0.1:1234"
	)
	cli, err := etcdutil.CreateClient([]string{t.endPoint}, nil)
	c.Assert(err, check.IsNil)
	defer cli.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e, err := NewElection(ctx, cli, sessionTTL, key, ID, addr, t.notifyBlockTime)
	c.Assert(err, check.IsNil)
	defer e.Close()

	// should become the leader
	select {
	case leader := <-e.LeaderNotify():
		c.Assert(leader.ID, check.Equals, ID)
	case <-time.After(timeout):
		c.Fatal("leader campaign timeout")
	}
	c.Assert(e.IsLeader(), check.IsTrue)
	leaderKey, leaderID, leaderAddr, err := e.LeaderInfo(ctx)
	c.Assert(err, check.IsNil)
	c.Assert(leaderID, check.Equals, e.ID())
	c.Assert(leaderAddr, check.Equals, addr)

	// the leader retired after deleted the key
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		select {
		case err2 := <-e.ErrorNotify():
			c.Fatalf("delete the leader key should not get an error, %v", err2)
		case leader := <-e.LeaderNotify():
			c.Assert(leader, check.IsNil)
		}
	}()
	_, err = cli.Delete(ctx, leaderKey)
	c.Assert(err, check.IsNil)
	wg.Wait()
}

func (t *testElectionSuite) TestElectionSucceedButReturnError(c *check.C) {
	var (
		timeout    = 5 * time.Second
		sessionTTL = 60
		key        = "unit-test/election-succeed-but-return-error"
		ID1        = "member1"
		ID2        = "member2"
		addr1      = "127.0.0.1:1"
		addr2      = "127.0.0.1:2"
	)
	cli, err := etcdutil.CreateClient([]string{t.endPoint}, nil)
	c.Assert(err, check.IsNil)
	defer cli.Close()

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()

	e1, err := NewElection(ctx1, cli, sessionTTL, key, ID1, addr1, t.notifyBlockTime)
	c.Assert(err, check.IsNil)
	defer e1.Close()

	// e1 should become the leader
	select {
	case leader := <-e1.LeaderNotify():
		c.Assert(leader.ID, check.Equals, ID1)
	case <-time.After(timeout):
		c.Fatal("leader campaign timeout")
	}
	c.Assert(e1.IsLeader(), check.IsTrue)
	_, leaderID, leaderAddr, err := e1.LeaderInfo(ctx1)
	c.Assert(err, check.IsNil)
	c.Assert(leaderID, check.Equals, e1.ID())
	c.Assert(leaderAddr, check.Equals, addr1)

	// start e2
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	e2, err := NewElection(ctx2, cli, sessionTTL, key, ID2, addr2, t.notifyBlockTime)
	c.Assert(err, check.IsNil)
	defer e2.Close()
	select {
	case leader := <-e2.leaderCh:
		c.Assert(leader.ID, check.Equals, ID1)
	case <-time.After(timeout):
		c.Fatal("leader campaign timeout")
	}
	// but the leader should still be e1
	_, leaderID, leaderAddr, err = e2.LeaderInfo(ctx2)
	c.Assert(err, check.IsNil)
	c.Assert(leaderID, check.Equals, e1.ID())
	c.Assert(leaderAddr, check.Equals, addr1)
	c.Assert(e2.IsLeader(), check.IsFalse)

	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/pkg/election/mockCapaignSucceedButReturnErr", `return()`), check.IsNil)
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/pkg/election/mockCapaignSucceedButReturnErr")

	e1.Close() // stop the campaign for e1
	c.Assert(e1.IsLeader(), check.IsFalse)

	// e2 should become the leader
	select {
	case leader := <-e2.LeaderNotify():
		c.Assert(leader.ID, check.Equals, ID2)
	case <-time.After(timeout):
		c.Fatal("leader campaign timeout")
	}

	// the leader retired after deleted the key
	select {
	case err2 := <-e2.ErrorNotify():
		c.Fatalf("delete the leader key should not get an error, %v", err2)
	case leader := <-e2.LeaderNotify():
		c.Assert(leader, check.IsNil)
	case <-time.After(timeout):
		c.Fatal("leader retire timeout")
	}
}
