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
	"fmt"
)

type Error struct {
	Message string
	Record  uint32
	Column  string
}

func (e Error) String() string {
	if e.Column != "" {
		return fmt.Sprintf(
			"Record %d, Column %s: %s",
			e.Record,
			e.Column,
			e.Message,
		)
	} else if e.Record != 0 {
		return fmt.Sprintf("Record %d: %s", e.Record, e.Message)
	} else {
		return e.Message
	}
}

type ErrorCollection struct {
	Errors map[string][]Error
}

func (ec ErrorCollection) FileError(
	file string,
	message string,
	params ...interface{},
) {
	ec.ValueError(file, 0, "", message, params...)
}

func (ec ErrorCollection) RecordError(
	file string,
	record uint32,
	message string,
	params ...interface{},
) {
	ec.ValueError(file, record, "", message, params...)
}

func (ec ErrorCollection) ValueError(
	file string,
	record uint32,
	column string,
	message string,
	params ...interface{},
) {
	_, ok := ec.Errors[file]
	if !ok {
		ec.Errors[file] = make([]Error, 0)
	}
	ec.Errors[file] = append(
		ec.Errors[file],
		Error{
			Message: fmt.Sprintf(message, params...),
			Record:  record,
			Column:  column,
		},
	)
}

func (ec ErrorCollection) HasErrors() bool {
	for file := range ec.Errors {
		if ec.FileHasErrors(file) {
			return true
		}
	}
	return false
}

func (ec ErrorCollection) FileHasErrors(file string) bool {
	errors, ok := ec.Errors[file]
	if ok {
		return len(errors) > 0
	}
	return false
}

func (ec ErrorCollection) GetFiles() []string {
	files := make([]string, 0, len(ec.Errors))
	for file := range ec.Errors {
		files = append(files, file)
	}
	return files
}

func NewErrorCollection() ErrorCollection {
	return ErrorCollection{
		Errors: make(map[string][]Error),
	}
}
