// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

package engine

// Binding is the application's committed identity projection. Retirement wins
// over every genesis/join grace rule and is never inferred from network input.
type Binding struct {
	ID        uint64 `json:"id"`
	Principal string `json:"principal"`
	Data      string `json:"data"`
	Admin     string `json:"admin"`
	Retired   bool   `json:"retired"`
}

// IdentitySource publishes only the small Meta identity view. It is invoked by
// the application executor, never by the heartbeat loop or socket goroutines.
type IdentitySource interface{ Identities() ([]Binding, error) }

func (r *Runtime) setBindings(bindings []Binding) {
	r.bindings = make(map[uint64]Binding, len(bindings))
	for _, binding := range bindings {
		r.bindings[binding.ID] = binding
	}
}

func (r *Runtime) authorizedMember(member Member) bool {
	if !r.checkIdentities {
		return true
	}
	if binding, ok := r.bindings[member.ID]; ok {
		return !binding.Retired && binding.Principal == member.Principal && binding.Data == member.Data && binding.Admin == member.Admin
	}
	complete := len(r.disk.genesis.Initial) > 0
	for _, initial := range r.disk.genesis.Initial {
		if _, ok := r.bindings[initial.ID]; !ok {
			complete = false
			break
		}
	}
	if !complete {
		for _, initial := range r.disk.genesis.Initial {
			if initial == member {
				return true
			}
		}
	}
	if seed := r.disk.join; seed != nil && r.core.applied < seed.Index {
		for _, invited := range seed.Members {
			if invited == member {
				return true
			}
		}
	}
	return false
}
