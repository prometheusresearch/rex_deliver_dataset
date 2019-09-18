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

package rexdeliverdataset_test

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	rdd "github.com/prometheusresearch/rex_deliver_dataset"
)

var _ = Describe("Utility Functions", func() {
	Describe("AbsPath", func() {
		It("Resolve tildes", func() {
			path, err := rdd.AbsPath("~/foobar")
			Expect(err).To(Succeed())
			Expect(path).To(HavePrefix("/"))
			Expect(path).To(HaveSuffix("/foobar"))
		})
	})

	Describe("FormatBytes", func() {
		It("Handles B", func() {
			out := rdd.FormatBytes(float64(123))
			Expect(out).To(Equal("123B"))
		})

		It("Handles K", func() {
			out := rdd.FormatBytes(float64(123456))
			Expect(out).To(Equal("120.56KiB"))
		})

		It("Handles Whole K", func() {
			out := rdd.FormatBytes(float64(1024 * 12))
			Expect(out).To(Equal("12KiB"))
		})

		It("Handles M", func() {
			out := rdd.FormatBytes(float64(123456789))
			Expect(out).To(Equal("117.74MiB"))
		})

		It("Handles Whole M", func() {
			out := rdd.FormatBytes(float64(1024 * 1024 * 12))
			Expect(out).To(Equal("12MiB"))
		})

		It("Handles G", func() {
			out := rdd.FormatBytes(float64(123456789123))
			Expect(out).To(Equal("114.98GiB"))
		})

		It("Handles Whole G", func() {
			out := rdd.FormatBytes(float64(1024 * 1024 * 1024 * 12))
			Expect(out).To(Equal("12GiB"))
		})

		It("Handles T", func() {
			out := rdd.FormatBytes(float64(123456789123456))
			Expect(out).To(Equal("112.28TiB"))
		})

		It("Handles Whole T", func() {
			out := rdd.FormatBytes(float64(1024 * 1024 * 1024 * 1024 * 12))
			Expect(out).To(Equal("12TiB"))
		})
	})

	Describe("TimeAsISO8601", func() {
		It("Works", func() {
			t := time.Date(2009, time.November, 10, 12, 34, 56, 0, time.UTC)
			Expect(rdd.TimeAsISO8601(t)).To(Equal("2009-11-10T12:34:56Z"))

			newYork, _ := time.LoadLocation("America/New_York")
			t = time.Date(2009, time.November, 10, 12, 34, 56, 0, newYork)
			Expect(rdd.TimeAsISO8601(t)).To(Equal("2009-11-10T12:34:56-0500"))
		})
	})
})
