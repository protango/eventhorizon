// Copyright (c) 2014 - Max Ekman <max@looplab.se>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package version

import (
	"context"
	"time"

	"github.com/jpillora/backoff"
	eh "github.com/looplab/eventhorizon"
)

// Repo is a middleware that adds version checking to a read repository.
type Repo struct {
	eh.ReadWriteRepo
}

// NewRepo creates a new Repo.
func NewRepo(repo eh.ReadWriteRepo) *Repo {
	return &Repo{
		ReadWriteRepo: repo,
	}
}

// Parent implements the Parent method of the eventhorizon.ReadRepo interface.
func (r *Repo) Parent() eh.ReadRepo {
	return r.ReadWriteRepo
}

// Find implements the Find method of the eventhorizon.ReadModel interface.
// If the context contains a min version set by WithMinVersion it will only
// return an item if its version is at least min version. If a timeout or
// deadline is set on the context it will repetedly try to get the item until
// either the version matches or the deadline is reached.
func (r *Repo) Find(ctx context.Context, id eh.UUID) (interface{}, error) {
	// If there is no min version set just return the item as normally.
	minVersion, ok := eh.MinVersionFromContext(ctx)
	if !ok || minVersion < 1 {
		return r.ReadWriteRepo.Find(ctx, id)
	}

	// Get a deadline to use when retrying.
	deadline, hasDeadline := ctx.Deadline()

	// Try to get a model with a min version.
	model, err := r.findMinVersion(ctx, id, minVersion)

	// Return the model if we found it on the first try.
	// Without a deadline it ends here event if there was an error.
	if err == nil || !hasDeadline {
		return model, err
	}

	// If we have a deadline but the error is a real error return it here.
	if err != nil {
		if rrErr, ok := err.(eh.RepoError); ok &&
			!(rrErr.Err == eh.ErrIncorrectModelVersion ||
				(rrErr.Err == eh.ErrModelNotFound && minVersion == 1)) {
			return nil, err
		}
	}

	// Try to get the model and retry with exponentially longer
	// intervals until the deadline expires.
	delay := &backoff.Backoff{
		Max: deadline.Sub(time.Now()),
	}
	for {
		select {
		case <-time.After(delay.Duration()):
			model, err := r.findMinVersion(ctx, id, minVersion)
			if rrErr, ok := err.(eh.RepoError); ok &&
				(rrErr.Err == eh.ErrIncorrectModelVersion ||
					(rrErr.Err == eh.ErrModelNotFound && minVersion == 1)) {
				// Try another time for incorrect min versions and for the
				// first creation of items.
				continue
			} else if err != nil {
				return nil, err
			}
			return model, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// findMinVersion finds an item if it has a version and it is at least minVersion.
func (r *Repo) findMinVersion(ctx context.Context, id eh.UUID, minVersion int) (interface{}, error) {
	model, err := r.ReadWriteRepo.Find(ctx, id)
	if err != nil {
		return nil, err
	}

	versionable, ok := model.(eh.Versionable)
	if !ok {
		return nil, eh.RepoError{
			Err:       eh.ErrModelHasNoVersion,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	if versionable.AggregateVersion() < minVersion {
		return nil, eh.RepoError{
			Err:       eh.ErrIncorrectModelVersion,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	return model, nil
}

// Repository returns a parent ReadRepo if there is one.
func Repository(repo eh.ReadRepo) *Repo {
	if repo == nil {
		return nil
	}

	if r, ok := repo.(*Repo); ok {
		return r
	}

	return Repository(repo.Parent())
}