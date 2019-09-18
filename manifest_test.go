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

var _ = Describe("Manifest", func() {
	Describe("CreateManifest", func() {
		It("Works", func() {
			config := rdd.Configuration{
				DatasetType: "omop-5.2-csv",
			}
			files := []rdd.File{
				{
					Name:     "foo.ext",
					FullPath: "/full/path/to/foo.ext",
					Size:     12345,
					Hash:     "ABC123",
				},
			}
			manifest := rdd.CreateManifest(config, files)

			Expect(manifest.Files).To(HaveLen(1))
			Expect(manifest.Files[0].Name).To(Equal("foo.ext"))
			Expect(manifest.Files[0].Size).To(BeNumerically("==", 12345))
			Expect(manifest.Files[0].Sha512).To(Equal("ABC123"))
			Expect(manifest.DateCreated).To(Not(BeNil()))
			Expect(manifest.DatasetType).To(Equal("omop-5.2-csv"))
		})
	})

	Describe("ToJSON", func() {
		It("Works", func() {
			config := rdd.Configuration{
				ExecutionTime: time.Date(2009, time.November, 10, 12, 34, 56, 0, time.UTC),
				DatasetType:   "omop-5.2-csv",
			}
			files := []rdd.File{
				{
					Name:     "foo.ext",
					FullPath: "/full/path/to/foo.ext",
					Size:     12345,
					Hash:     "ABC123",
				},
			}
			manifest := rdd.CreateManifest(config, files)
			manifest.Generator = "just a test"

			json, err := manifest.ToJSON()
			Expect(err).To(Succeed())
			Expect(string(json)).To(Equal(`{"date_created":"2009-11-10T12:34:56Z","dataset_type":"omop-5.2-csv","generator":"just a test","files":[{"name":"foo.ext","size":12345,"sha512":"ABC123"}]}`))
		})
	})
})
