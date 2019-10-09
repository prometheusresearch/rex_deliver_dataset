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
	"runtime"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	rdd "github.com/prometheusresearch/rex_deliver_dataset"
)

var _ = Describe("Config", func() {
	Describe("NewConfiguration", func() {
		It("Works", func() {
			cfg := rdd.NewConfiguration()
			Expect(cfg.ExecutionTime).To(Not(BeNil()))
			Expect(cfg.Storage).To(Not(BeNil()))
		})
	})

	Describe("Validate", func() {
		It("Checks Basic Storage Props", func() {
			cfg := rdd.NewConfiguration()
			cfg.DatasetType = "omop:5.2:csv"

			err := cfg.Validate()
			Expect(err).To(MatchError("storage requires kind property"))

			cfg.Storage["kind"] = "gs"
			err = cfg.Validate()
			Expect(err).To(MatchError("storage requires container property"))

			cfg.Storage["container"] = "test"
			cfg.Storage["credentials_json"] = "/some/file.json"
			err = cfg.Validate()
			Expect(err).To(Succeed())
		})

		It("Checks S3 Storage", func() {
			cfg := rdd.NewConfiguration()
			cfg.DatasetType = "omop:5.2:csv"
			cfg.Storage["kind"] = "s3"
			cfg.Storage["container"] = "test"

			err := cfg.Validate()
			Expect(err).To(MatchError("storage requires access_key property when kind=s3"))

			cfg.Storage["access_key"] = "foo"
			err = cfg.Validate()
			Expect(err).To(MatchError("storage requires secret_key property when kind=s3"))

			cfg.Storage["secret_key"] = "bar"
			err = cfg.Validate()
			Expect(err).To(MatchError("storage requires region property when kind=s3"))

			cfg.Storage["region"] = "baz"
			err = cfg.Validate()
			Expect(err).To(Succeed())
		})

		It("Checks GCS Storage", func() {
			cfg := rdd.NewConfiguration()
			cfg.DatasetType = "omop:5.2:csv"
			cfg.Storage["kind"] = "gs"
			cfg.Storage["container"] = "test"

			err := cfg.Validate()
			Expect(err).To(MatchError("storage requires credentials_json property when kind=gs"))

			cfg.Storage["credentials_json"] = "foo"
			err = cfg.Validate()
			Expect(err).To(Succeed())
		})

		It("Checks Local Storage", func() {
			cfg := rdd.NewConfiguration()
			cfg.DatasetType = "omop:5.2:csv"
			cfg.Storage["kind"] = "local"
			cfg.Storage["container"] = "test"

			err := cfg.Validate()

			if runtime.GOOS == "windows" {
				Expect(err).To(MatchError("storage.kind cannot be local on Windows systems"))

			} else {
				Expect(err).To(MatchError("storage requires path property when kind=local"))

				cfg.Storage["path"] = "/foo"
				err = cfg.Validate()
				Expect(err).To(Succeed())
			}
		})

		It("Handles Unknown Kind", func() {
			cfg := rdd.NewConfiguration()
			cfg.DatasetType = "omop:5.2:csv"
			cfg.Storage["kind"] = "foo"
			cfg.Storage["container"] = "test"

			err := cfg.Validate()
			Expect(err).To(MatchError("storage.kind must be one of: gs, local, s3"))
		})

		It("Handles Bad Path", func() {
			cfg := rdd.NewConfiguration()
			cfg.DatasetType = "omop:5.2:csv"
			cfg.Storage["kind"] = "gs"
			cfg.Storage["container"] = "test"
			cfg.Storage["credentials_json"] = "/some/file.json"
			cfg.Storage["path"] = "foo"

			err := cfg.Validate()
			Expect(err).To(MatchError("storage.path must be an absolute path"))
		})

		It("Handles Bad Dataset Type", func() {
			cfg := rdd.NewConfiguration()
			cfg.Storage["kind"] = "gs"
			cfg.Storage["container"] = "test"
			cfg.Storage["credentials_json"] = "/some/file.json"
			cfg.DatasetType = "bar"

			err := cfg.Validate()
			Expect(err).To(MatchError("dataset_type must be one of: omop:5.2:csv"))
		})

		It("Handles Missing Dataset Type", func() {
			cfg := rdd.NewConfiguration()
			cfg.Storage["kind"] = "gs"
			cfg.Storage["container"] = "test"
			cfg.Storage["credentials_json"] = "/some/file.json"

			err := cfg.Validate()
			Expect(err).To(MatchError("dataset_type must be one of: omop:5.2:csv"))
		})
	})

	Describe("ReadConfig", func() {
		It("Works", func() {
			content := []byte("{dataset_type: omop:5.2:csv, storage: {kind: s3, container: test, access_key: foo, secret_key: bar, region: baz}}")
			file := makeTempFile(content)
			defer os.Remove(file.Name())

			cfg, err := rdd.ReadConfig(file.Name())
			Expect(err).To(Succeed())
			Expect(cfg.ConfigurationPath).To(Equal(file.Name()))
			Expect(cfg.DatasetType).To(Equal("omop:5.2:csv"))
			Expect(cfg.Storage["kind"]).To(Equal("s3"))
			Expect(cfg.Storage["container"]).To(Equal("test"))
			Expect(cfg.Storage["access_key"]).To(Equal("foo"))
			Expect(cfg.Storage["secret_key"]).To(Equal("bar"))
			Expect(cfg.Storage["region"]).To(Equal("baz"))
		})

		It("Handles missing files", func() {
			_, err := rdd.ReadConfig("./doesntexist")
			Expect(err).To(Not(Succeed()))
		})

		It("Handles bogus YAML", func() {
			content := []byte("{garbage")
			file := makeTempFile(content)
			defer os.Remove(file.Name())

			_, err := rdd.ReadConfig(file.Name())
			Expect(err).To(Not(Succeed()))
		})
	})
})
