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
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	rdd "github.com/prometheusresearch/rex_deliver_dataset"
)

var _ = Describe("Reader", func() {
	Describe("CreateFileReader", func() {
		It("Works", func() {
			content := []byte("foobar")
			file := makeTempFile(content)
			defer os.Remove(file.Name())

			reader, err := rdd.CreateFileReader(file.Name())
			Expect(err).To(Succeed())
			buf := make([]byte, 6)
			reader.Read(buf)
			Expect(buf).To(Equal([]byte("foobar")))
		})

		It("Handles missing files", func() {
			reader, err := rdd.CreateFileReader("./doesntexist")
			Expect(err).To(Not(Succeed()))
			Expect(reader).To(BeNil())
		})
	})

	Describe("GetHash", func() {
		It("Works", func() {
			content := []byte("foobar")
			file := makeTempFile(content)
			defer os.Remove(file.Name())

			reader, err := rdd.CreateFileReader(file.Name())
			Expect(err).To(Succeed())
			buf := make([]byte, 6)
			reader.Read(buf)

			Expect(reader.GetHash()).To(Equal("0a50261ebd1a390fed2bf326f2673c145582a6342d523204973d0219337f81616a8069b012587cf5635f6925f1b56c360230c19b273500ee013e030601bf2425"))
		})
	})
})
