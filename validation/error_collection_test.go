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

var _ = Describe("ErrorCollection", func() {
	Describe("Error", func() {
		It("Renders as string", func() {
			err := val.Error{
				Message: "An error",
			}
			Expect(err.String()).To(Equal("An error"))

			err.Record = 42
			Expect(err.String()).To(Equal("Record 42: An error"))

			err.Column = "SOME_COL"
			Expect(err.String()).To(Equal("Record 42, Column SOME_COL: An error"))
		})
	})

	Describe("FileError", func() {
		It("Captures errors", func() {
			ec := val.NewErrorCollection()
			Expect(ec.Errors).To(HaveLen(0))

			ec.FileError("foo.ext", "An error")
			Expect(ec.Errors).To(HaveLen(1))
			Expect(ec.Errors["foo.ext"]).To(ConsistOf(
				val.Error{
					Message: "An error",
					Record:  0,
					Column:  "",
				},
			))

			ec.FileError("foo.ext", "Another error!")
			Expect(ec.Errors).To(HaveLen(1))
			Expect(ec.Errors["foo.ext"]).To(ConsistOf(
				val.Error{
					Message: "An error",
					Record:  0,
					Column:  "",
				},
				val.Error{
					Message: "Another error!",
					Record:  0,
					Column:  "",
				},
			))
		})
	})

	Describe("RecordError", func() {
		It("Captures errors", func() {
			ec := val.NewErrorCollection()
			Expect(ec.Errors).To(HaveLen(0))

			ec.RecordError("foo.ext", 123, "An error")
			Expect(ec.Errors).To(HaveLen(1))
			Expect(ec.Errors["foo.ext"]).To(ConsistOf(
				val.Error{
					Message: "An error",
					Record:  123,
					Column:  "",
				},
			))

			ec.RecordError("foo.ext", 42, "Another error!")
			Expect(ec.Errors).To(HaveLen(1))
			Expect(ec.Errors["foo.ext"]).To(ConsistOf(
				val.Error{
					Message: "An error",
					Record:  123,
					Column:  "",
				},
				val.Error{
					Message: "Another error!",
					Record:  42,
					Column:  "",
				},
			))
		})
	})

	Describe("ValueError", func() {
		It("Captures errors", func() {
			ec := val.NewErrorCollection()
			Expect(ec.Errors).To(HaveLen(0))

			ec.ValueError("foo.ext", 123, "SOME_COLUMN", "An error")
			Expect(ec.Errors).To(HaveLen(1))
			Expect(ec.Errors["foo.ext"]).To(ConsistOf(
				val.Error{
					Message: "An error",
					Record:  123,
					Column:  "SOME_COLUMN",
				},
			))

			ec.ValueError("foo.ext", 42, "OTHER_COL", "Another error!")
			Expect(ec.Errors).To(HaveLen(1))
			Expect(ec.Errors["foo.ext"]).To(ConsistOf(
				val.Error{
					Message: "An error",
					Record:  123,
					Column:  "SOME_COLUMN",
				},
				val.Error{
					Message: "Another error!",
					Record:  42,
					Column:  "OTHER_COL",
				},
			))
		})
	})

	Describe("HasErrors", func() {
		It("Works", func() {
			ec := val.NewErrorCollection()

			Expect(ec.HasErrors()).To(Equal(false))
			ec.RecordError("foo.ext", 123, "An error")
			Expect(ec.HasErrors()).To(Equal(true))
		})
	})

	Describe("FileHasErrors", func() {
		It("Works", func() {
			ec := val.NewErrorCollection()

			Expect(ec.FileHasErrors("foo.ext")).To(Equal(false))
			ec.RecordError("foo.ext", 123, "An error")
			Expect(ec.FileHasErrors("foo.ext")).To(Equal(true))

			Expect(ec.FileHasErrors("bar.ext")).To(Equal(false))
			ec.ValueError("bar.ext", 123, "SOME_COLUMN", "An error")
			Expect(ec.FileHasErrors("bar.ext")).To(Equal(true))
		})
	})

	Describe("GetFiles", func() {
		It("Works", func() {
			ec := val.NewErrorCollection()

			Expect(ec.GetFiles()).To(BeEmpty())

			ec.FileError("foo.ext", "An error")
			Expect(ec.GetFiles()).To(ConsistOf("foo.ext"))

			ec.RecordError("bar.ext", 123, "An error")
			ec.RecordError("bar.ext", 456, "An error")
			Expect(ec.GetFiles()).To(ConsistOf("foo.ext", "bar.ext"))

			ec.ValueError("baz.ext", 456, "SOME_COLUMN", "An error")
			Expect(ec.GetFiles()).To(ConsistOf("foo.ext", "bar.ext", "baz.ext"))
		})
	})
})
