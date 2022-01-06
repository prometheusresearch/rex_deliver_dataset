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
	"path/filepath"
	"runtime"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	rdd "github.com/prometheusresearch/rex_deliver_dataset"
)

var _ = Describe("Catalog", func() {
	Describe("CatalogDirectory", func() {
		It("Works", func() {
			path, _ := rdd.AbsPath("./test_datasets/catalog")
			files, err := rdd.CatalogDirectory(path)
			Expect(err).To(Succeed())

			Expect(files).To(ContainElement(rdd.File{
				Name:     "foo",
				FullPath: filepath.Join(path, "foo"),
				Size:     6,
			}))

			Expect(files).To(ContainElement(rdd.File{
				Name:     "bar",
				FullPath: filepath.Join(path, "bar"),
				Size:     15,
			}))

			Expect(files).To(ContainElement(rdd.File{
				Name:     "subdir/baz",
				FullPath: filepath.Join(path, "subdir/baz"),
				Size:     26,
			}))

			Expect(files).To(HaveLen(3))
		})

		It("Handles missing directories", func() {
			path, _ := rdd.AbsPath("./test_datasets/doesntexist")
			files, err := rdd.CatalogDirectory(path)
			Expect(files).To(BeEmpty())
			Expect(err).To(Not(Succeed()))
		})

		It("Handles non-directories", func() {
			path, _ := rdd.AbsPath("./test_datasets/catalog/foo")
			files, err := rdd.CatalogDirectory(path)
			Expect(files).To(BeEmpty())
			Expect(err).To(Not(Succeed()))
			Expect(err.Error()).To(HaveSuffix("foo is not a directory"))
		})
	})
})
