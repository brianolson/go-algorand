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

func (as *addrSet) hasTxn(stxn transactions.SignedTxnWithAD) bool {
	return as.contains(stxn.Txn.Sender) ||
	as.contains(stxn.Txn.Receiver) ||
	as.contains(stxn.Txn.CloseRemainderTo) ||
	as.contains(stxn.Txn.AssetReceiver) ||
	as.contains(stxn.Txn.AssetCloseTo) ||
	as.contains(stxn.Txn.AssetSender) ||
	as.contains(stxn.Txn.FreezeAccount)
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
	dependsOn []int
}

func (pdg *paysetDependencyGroup) add(txgroup []transactions.SignedTxnWithAD) {
	pdg.paysetgroups = append(pdg.paysetgroups, txgroup)
	for _, stxn := range txgroup {
		pdg.addrs.addTxn(stxn)
	}
}

func (pdg *paysetDependencyGroup) hasAddrsOf(txgroup []transactions.SignedTxnWithAD) {
	for _, stxn := range txgroup {
		pdg.addrs.addTxn(stxn)
	}
}

func foo(paysetgroups [][]transactions.SignedTxnWithAD) {
	depgroups := make([]paysetDependencyGroup, 1, 10)
	//depgroups[0].add(paysetgroups[0])
	// TODO: AssetConfig (create or re-config) serializes with anything operating on that asset id (ConfigAsset, XferAsset, FreezeAsset)
	// TODO: app call ApplicationID serializes with anything on that ApplicationID

	for _, txgroup := range paysetgroups {
		dep := -1
		var dependsOn []int = nil
		for i, dg := range depgroups {
			if dg.hasAddrsOf(txgroup) {
			}
		}
	}

}
