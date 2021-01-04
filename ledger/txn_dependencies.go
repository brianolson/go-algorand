// Copyright (C) 2020 Algorand, Inc.
// This file is part of go-algorand
//
// go-algorand is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// go-algorand is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with go-algorand.  If not, see <https://www.gnu.org/licenses/>.

package ledger

import (
	"bytes"
	"encoding/binary"
	"time"

	"github.com/algorand/go-algorand/data/basics"
	"github.com/algorand/go-algorand/data/transactions"
)

type dependencyAddr struct {
	first8 uint64
	addr   basics.Address
}

func newDependencyAddr(a basics.Address) dependencyAddr {
	return dependencyAddr{
		binary.LittleEndian.Uint64(a[:]),
		a,
	}
}

func (da dependencyAddr) equals(b dependencyAddr) bool {
	if da.first8 != b.first8 {
		return false
	}
	return bytes.Equal(da.addr[:], b.addr[:])
}

type addrSet struct {
	they []dependencyAddr
}

func (as *addrSet) add(a basics.Address) {
	if a.IsZero() {
		return
	}
	nda := newDependencyAddr(a)
	for _, t := range as.they {
		if t.equals(nda) {
			return
		}
	}
	as.they = append(as.they, nda)
}

func (as *addrSet) addTxn(stxn transactions.SignedTxnWithAD) {
	as.add(stxn.Txn.Sender)
	as.add(stxn.Txn.Receiver)
	as.add(stxn.Txn.CloseRemainderTo)
	as.add(stxn.Txn.AssetReceiver)
	as.add(stxn.Txn.AssetCloseTo)
	as.add(stxn.Txn.AssetSender)
	as.add(stxn.Txn.FreezeAccount)
	for _, appAddr := range stxn.Txn.Accounts {
		as.add(appAddr)
	}
}

func (as *addrSet) contains(a basics.Address) bool {
	if a.IsZero() {
		return false
	}
	nda := newDependencyAddr(a)
	for _, t := range as.they {
		if t.equals(nda) {
			return true
		}
	}
	return false
}

func (as *addrSet) hasTxnAddr(stxn transactions.SignedTxnWithAD) bool {
	if as.contains(stxn.Txn.Sender) ||
		as.contains(stxn.Txn.Receiver) ||
		as.contains(stxn.Txn.CloseRemainderTo) ||
		as.contains(stxn.Txn.AssetReceiver) ||
		as.contains(stxn.Txn.AssetCloseTo) ||
		as.contains(stxn.Txn.AssetSender) ||
		as.contains(stxn.Txn.FreezeAccount) {
		return true
	}
	for _, appAddr := range stxn.Txn.Accounts {
		if as.contains(appAddr) {
			return true
		}
	}
	return false
}

type paysetDependencyGroup struct {
	// transaction groups
	paysetgroups [][]transactions.SignedTxnWithAD

	addrs addrSet

	// paysetDependencyGroup that must execute before this one
	dependsOn []*paysetDependencyGroup

	// existing assets that were modified by acfg
	assetsChanged []basics.AssetIndex

	// app ids called or configured
	appIds []basics.AppIndex

	// add nothing new here, it may have been sent for eval
	closed bool

	id int

	err error
}

func (pdg *paysetDependencyGroup) add(txgroup []transactions.SignedTxnWithAD) {
	pdg.paysetgroups = append(pdg.paysetgroups, txgroup)
	for _, stxn := range txgroup {
		pdg.addrs.addTxn(stxn)
		if stxn.Txn.ConfigAsset != 0 {
			// don't need to record zero ConfigAsset on asset create because nothing can use that number until it's public next round
			alreadyThere := false
			for _, aid := range pdg.assetsChanged {
				if aid == stxn.Txn.ConfigAsset {
					alreadyThere = true
					break
				}
			}
			if !alreadyThere {
				pdg.assetsChanged = append(pdg.assetsChanged, stxn.Txn.ConfigAsset)
			}
		}
		if stxn.Txn.ApplicationID != 0 {
			// don't need to record zero ApplicationID because nothing can use it until next round
			alreadyThere := false
			for _, aid := range pdg.appIds {
				if aid == stxn.Txn.ApplicationID {
					alreadyThere = true
					break
				}
			}
			if !alreadyThere {
				pdg.appIds = append(pdg.appIds, stxn.Txn.ApplicationID)
			}
		}
	}
}

// return true if elements in this dependency group could interact with things in the txgroup and imply a dependency
func (pdg *paysetDependencyGroup) mustPrecede(txgroup []transactions.SignedTxnWithAD) bool {
	for _, stxn := range txgroup {
		if pdg.addrs.hasTxnAddr(stxn) {
			return true
		}
		assetId := basics.AssetIndex(0)
		if stxn.Txn.ConfigAsset != 0 {
			assetId = stxn.Txn.ConfigAsset
		} else if stxn.Txn.XferAsset != 0 {
			assetId = stxn.Txn.XferAsset
		} else if stxn.Txn.FreezeAsset != 0 {
			assetId = stxn.Txn.FreezeAsset
		}
		if assetId != 0 {
			for _, aid := range pdg.assetsChanged {
				if aid == stxn.Txn.ConfigAsset {
					return true
				}
			}
		}
		if stxn.Txn.ApplicationID != 0 {
			for _, aid := range pdg.appIds {
				if aid == stxn.Txn.ApplicationID {
					return true
				}
			}
		}
	}
	return false
}

// for testing
func (pdg *paysetDependencyGroup) hasIntersection(dg *paysetDependencyGroup, logf func(format string, args ...interface{})) bool {
	for _, daddr := range dg.addrs.they {
		if pdg.addrs.contains(daddr.addr) {
			logf("addr %s", daddr.addr.String())
			return true
		}
	}
	for _, aa := range pdg.assetsChanged {
		for _, ab := range dg.assetsChanged {
			if aa == ab {
				return true
			}
		}
	}
	for _, pa := range pdg.appIds {
		for _, pb := range dg.appIds {
			if pa == pb {
				return true
			}
		}
	}
	return false
}

type logf interface {
	Logf(format string, args ...interface{})
}

type nopLogf struct{}

func (nlf *nopLogf) Logf(format string, args ...interface{}) {
}

var nopLogfSingleton nopLogf

var debugLogf logf = &nopLogfSingleton

/*
func buildDepGroups(paysetgroups [][]transactions.SignedTxnWithAD) (depgroups []paysetDependencyGroup) {
	depgroups = make([]paysetDependencyGroup, 0, 10)
	//depgroups[0].add(paysetgroups[0])
	// TODO: AssetConfig (create or re-config) serializes with anything operating on that asset id (ConfigAsset, XferAsset, FreezeAsset). (acfg.caid == 0) doesn't need to synchronize because practically nothing can depend on it till the next round.
	// TODO: app call ApplicationID serializes with anything on that ApplicationID

	for tgi, txgroup := range paysetgroups {
		dep := -1
		var dependsOn []int = nil
		for i, dg := range depgroups {
			if dg.mustPrecede(txgroup) {
				if dep == -1 {
					dep = i
				} else if i == dep {
				} else if len(dependsOn) == 0 {
					dependsOn = make([]int, 2)
					dependsOn[0] = dep
					dependsOn[1] = i
				} else {
					found := false
					for _, di := range dependsOn {
						if di == i {
							found = true
							break
						}
					}
					if !found {
						dependsOn = append(dependsOn)
					}
				}
			}
		}

		if dep != -1 && dependsOn == nil {
			// depends on one thing, append to that group
			depgroups[dep].add(txgroup)
		} else if dependsOn != nil {
			// depends on several things, new group
			debugLogf.Logf("tg[%d] new group, depends on %#v", tgi, dependsOn)
			npdg := paysetDependencyGroup{dependsOn: dependsOn}
			npdg.add(txgroup)
			depgroups = append(depgroups, npdg)
		} else {
			// depends on nothing. new group.
			debugLogf.Logf("tg[%d] new group", tgi)
			npdg := paysetDependencyGroup{}
			npdg.add(txgroup)
			depgroups = append(depgroups, npdg)
		}
	}
	return
        }
*/

type ringFifo struct {
	they []interface{}

	// next position to write to
	in int

	// next position to read from
	out int
}

func (rf *ringFifo) isFull() bool {
	return ((rf.in + 1) % len(rf.they)) == rf.out
}

func (rf *ringFifo) isEmpty() bool {
	return rf.in == rf.out
}

func (rf *ringFifo) put(x interface{}) {
	rf.they[rf.in] = x
	rf.in = (rf.in + 1) % len(rf.they)
}

func (rf *ringFifo) pop() (v interface{}, ok bool) {
	if rf.in == rf.out {
		return nil, false
	}
	v = rf.they[rf.out]
	ok = true
	rf.out = (rf.out + 1) % len(rf.they)
	return
}

/*
func (rf *ringFifo) contains(x interface{}) bool {
	i := rf.out
	for i != rf.in {
		if rf.they[i] == x {
			return true
		}
		i = (i + 1) % len(rf.they)
	}
	return false
}
*/

type actionFlags int

const (
	noAction    actionFlags = 0
	actionPop   actionFlags = 1
	quitForeach actionFlags = 2
)

func (rf *ringFifo) foreach(visitor func(x interface{}) actionFlags) {
	i := rf.out
	for i != rf.in {
		action := visitor(rf.they[i])
		if (action & actionPop) != 0 {
			source := (i + 1) % len(rf.they)
			dest := i
			for source != rf.in {
				rf.they[dest] = rf.they[source]
				source = (source + 1) % len(rf.they)
				dest = (dest + 1) % len(rf.they)
			}
			rf.in = (rf.in + len(rf.they) - 1) % len(rf.they)
		} else {
			i = (i + 1) % len(rf.they)
		}
		if (action & quitForeach) != 0 {
			return
		}
	}
}

type depGroupRunner struct {
	dgi int

	// dependency groups being analyzed and added to
	dgFifo ringFifo

	// dependency groups being run by evaluator
	inProcessing []*paysetDependencyGroup

	// send to processing threads
	todo chan *paysetDependencyGroup

	// return from processing threads
	done chan *paysetDependencyGroup

	// next on todo
	nextToSend *paysetDependencyGroup

	err error
}

func (dgr *depGroupRunner) nextForProcessing() (next *paysetDependencyGroup) {
	dgr.dgFifo.foreach(func(x interface{}) actionFlags {
		dg := x.(*paysetDependencyGroup)
		for _, dgp := range dg.dependsOn {
			for _, pp := range dgr.inProcessing {
				if pp == dgp {
					return noAction
				}
			}
		}
		next = dg
		return actionPop | quitForeach
	})
	return
}
func (dgr *depGroupRunner) removeFromInProcessing(dg *paysetDependencyGroup) {
	debugLogf.Logf("done %p %d", dg, dg.id)
	for i, v := range dgr.inProcessing {
		if v == dg {
			end := len(dgr.inProcessing) - 1
			dgr.inProcessing[i] = dgr.inProcessing[end]
			dgr.inProcessing[end] = nil // Go GC can count references past end of active slice
			dgr.inProcessing = dgr.inProcessing[:end]
			if dg.err != nil {
				dgr.err = dg.err
			}
			return
		}
	}
	debug("removeFromInProcessing but not inProcessing %#v", dg)
}

/*
func (dgr *depGroupRunner) processDone() {
	select {
	case dg := <-dgr.done:
		dgr.removeFromInProcessing(dg)
	default:
	}
}
*/

func (dgr *depGroupRunner) processTodoDone() {
	if dgr.nextToSend == nil {
		dgr.nextToSend = dgr.nextForProcessing()
		id := -1
		if dgr.nextToSend != nil {
			id = dgr.nextToSend.id
			debugLogf.Logf("next to send %p %d", dgr.nextToSend, id)
		}
	}
	var dg *paysetDependencyGroup
	if dgr.nextToSend == nil {
		select {
		case dg = <-dgr.done:
			dgr.removeFromInProcessing(dg)
		default:
		}
	} else if len(dgr.inProcessing) == 0 {
		select {
		case dgr.todo <- dgr.nextToSend:
			dgr.inProcessing = append(dgr.inProcessing, dgr.nextToSend)
			dgr.nextToSend = nil
		default:
		}
	} else {
		select {
		case dg = <-dgr.done:
			dgr.removeFromInProcessing(dg)
		case dgr.todo <- dgr.nextToSend:
			dgr.inProcessing = append(dgr.inProcessing, dgr.nextToSend)
			dgr.nextToSend = nil
		}
	}
}
func (dgr *depGroupRunner) runDepGroups(paysetgroups [][]transactions.SignedTxnWithAD) error {
	dgr.dgFifo.they = make([]interface{}, 10)
	dref := make([]*paysetDependencyGroup, 0, 10)

	noSendCounter := 0
	incount := 0
	nodep := 0
	mdep := 0
	for tgi, txgroup := range paysetgroups {
		incount++
		for dgr.dgFifo.isFull() {
			dgr.processTodoDone()
			noSendCounter = 0
			if dgr.err != nil {
				return dgr.err
			}
		}
		if noSendCounter > 5 || len(dgr.inProcessing) == 0 {
			dgr.processTodoDone()
			if dgr.err != nil {
				return dgr.err
			}
		}
		dref = dref[:0]
		ipc := 0
		for _, dg := range dgr.inProcessing {
			if dg.mustPrecede(txgroup) {
				dref = append(dref, dg)
				ipc++
			}
		}
		dgf := 0
		dgr.dgFifo.foreach(func(x interface{}) actionFlags {
			dg := x.(*paysetDependencyGroup)
			if dg.mustPrecede(txgroup) {
				dref = append(dref, dg)
				dgf++
			}
			return noAction
		})

		if len(dref) == 0 {
			// depends on nothing. new group.
			nodep++
			debugLogf.Logf("tg[%d] new group, no deps", tgi)
			npdg := &paysetDependencyGroup{id: dgr.dgi}
			dgr.dgi++
			npdg.add(txgroup)
			dgr.dgFifo.put(npdg)
		} else if len(dref) == 1 && ipc == 0 {
			// depends on one thing (which is not already in processing) append to that group
			dref[0].add(txgroup)
		} else {
			// depends on several things, new group
			mdep++
			dependsOn := make([]*paysetDependencyGroup, len(dref))
			copy(dependsOn, dref)
			debugLogf.Logf("tg[%d] new group, depends on %#v", tgi, dependsOn)
			npdg := &paysetDependencyGroup{dependsOn: dependsOn, id: dgr.dgi}
			dgr.dgi++
			npdg.add(txgroup)
			dgr.dgFifo.put(npdg)
		}
		noSendCounter++
	}
	debugLogf.Logf("runDepGroups %d groups, %d nodep, %d mdep", incount, nodep, mdep)
	// finish sending things todo
	for !dgr.dgFifo.isEmpty() {
		dgr.processTodoDone()
		if dgr.err != nil {
			return dgr.err
		}
		debugLogf.Logf("dgr.dgFifo.isEmpty()=%v len(dgr.inProcessing)=%d", dgr.dgFifo.isEmpty(), len(dgr.inProcessing))
		time.Sleep(10 * time.Millisecond)
	}
	close(dgr.todo)
	//for dg := range dgr.done {
	for len(dgr.inProcessing) > 0 {
		dg, ok := <-dgr.done
		if !ok {
			break
		}
		dgr.removeFromInProcessing(dg)
	}
	// TODO: check that everything finished nicely
	return dgr.err
}
