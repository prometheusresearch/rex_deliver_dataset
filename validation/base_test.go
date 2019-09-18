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

package validation_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	val "github.com/prometheusresearch/rex_deliver_dataset/validation"
)

func testVal(path string, files []string) val.ErrorCollection {
	return val.ErrorCollection{}
}

var _ = Describe("Registry", func() {
	It("Allows registering of validator functions", func() {
		types := val.GetAvailableTypes()
		Expect(types).To(BeEmpty())
		validator := val.NewValidator("foo")
		Expect(validator).To(BeNil())

		val.Register("foo", testVal)

		types = val.GetAvailableTypes()
		Expect(types).To(ConsistOf("foo"))
		validator = val.NewValidator("foo")
		Expect(validator).To(Not(BeNil()))
	})
})
