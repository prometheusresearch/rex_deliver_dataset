/*
	RexRegistry Dataset Delivery
    Copyright (C) 2019 Prometheus Research, LLC

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package validation

import (
	"sort"
	"sync"
)

type Validator func(path string, files []string) ErrorCollection

var (
	regLock  sync.RWMutex
	registry map[string]Validator
)

func Register(name string, validator Validator) {
	regLock.Lock()
	registry[name] = validator
	regLock.Unlock()
}

func NewValidator(datasetType string) Validator {
	regLock.Lock()
	defer regLock.Unlock()
	return registry[datasetType]
}

func GetAvailableTypes() []string {
	regLock.Lock()
	types := make([]string, 0, len(registry))
	for key := range registry {
		types = append(types, key)
	}
	regLock.Unlock()
	sort.Strings(types)
	return types
}

func init() {
	registry = make(map[string]Validator)
}
