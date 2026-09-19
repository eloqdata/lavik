// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

package engine

import "testing"

func bindingFor(m Member) Binding {
	return Binding{ID: m.ID, Principal: m.Principal, Data: m.Data, Admin: m.Admin}
}

func TestGenesisGraceCannotOverrideCommittedIdentity(t *testing.T) {
	members := testMembers()
	r := &Runtime{core: testCore(t, 1), disk: &diskStore{genesis: genesis{Initial: members}}, checkIdentities: true}
	r.setBindings(nil)
	if !r.knownPeer(2) {
		t.Fatal("pristine genesis descriptor rejected")
	}
	changed := members[1]
	changed.Admin = "127.0.0.1:9999"
	if r.authorizedMember(changed) {
		t.Fatal("genesis grace widened to a different endpoint")
	}
	retired := bindingFor(members[1])
	retired.Retired = true
	r.setBindings([]Binding{retired})
	if r.knownPeer(2) {
		t.Fatal("partial genesis resurrected a retired identity")
	}
	r.setBindings([]Binding{bindingFor(members[0]), retired, bindingFor(members[2])})
	if r.knownPeer(2) || !r.knownPeer(3) {
		t.Fatal("replayed bindings changed authorization")
	}
	// Applying a descriptor update without the matching identity never grants
	// temporary grace, even when the old descriptor was an initial voter.
	changed = members[2]
	changed.Admin = "127.0.0.1:9999"
	r.core.members[3] = changed
	if r.knownPeer(3) {
		t.Fatal("descriptor change reopened genesis grace")
	}
}

func TestJoinGraceEndsAtAppliedConfigurationCut(t *testing.T) {
	members := testMembers()
	r := &Runtime{core: testCore(t, 1), disk: &diskStore{join: &joinSeed{Index: 10, Members: members}}, checkIdentities: true}
	r.core.members = map[uint64]Member{}
	r.setBindings(nil)
	if !r.knownPeer(2) {
		t.Fatal("durable invitation did not authorize catch-up")
	}
	retired := bindingFor(members[1])
	retired.Retired = true
	r.setBindings([]Binding{retired})
	if r.knownPeer(2) {
		t.Fatal("invitation overrode retirement below its cut")
	}
	r.setBindings(nil)
	r.core.applied = 10
	if r.knownPeer(2) {
		t.Fatal("invitation remained active after its applied cut")
	}
	r.core.members[2] = members[1]
	if r.knownPeer(2) {
		t.Fatal("installed membership without identity reopened grace")
	}
	r.setBindings([]Binding{bindingFor(members[1])})
	if !r.knownPeer(2) {
		t.Fatal("matching committed descriptor and identity rejected")
	}
	delete(r.core.members, 2)
	if r.knownPeer(2) {
		t.Fatal("removed member recovered authority through invitation")
	}
}
