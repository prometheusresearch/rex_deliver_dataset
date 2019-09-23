// +build !windows

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
	"io/ioutil"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	rdd "github.com/prometheusresearch/rex_deliver_dataset"
)

func makeTempConfig() rdd.Configuration {
	config := rdd.NewConfiguration()
	config.Storage["kind"] = "local"
	config.Storage["path"] = tmpdir()
	config.Storage["container"] = "justatest"
	return config
}

func findFile(path string, name string) string {
	var fullPath string

	filepath.Walk(
		path,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.Mode().IsRegular() {
				fullPath = path
			}
			return nil
		},
	)

	return fullPath
}

func getFileContent(path string) string {
	content, _ := ioutil.ReadFile(path)
	return string(content)
}

var _ = Describe("Upload", func() {
	Describe("NewUploader", func() {
		It("Works", func() {
			uploader, err := rdd.NewUploader(makeTempConfig())
			Expect(err).To(Succeed())
			Expect(uploader).To(Not(BeNil()))
		})

		It("Handles bad config", func() {
			config := rdd.NewConfiguration()
			config.Storage["kind"] = "sneakernet"
			config.Storage["container"] = "justatest"

			uploader, err := rdd.NewUploader(config)
			Expect(err).To(MatchError("Unknown storage kind: sneakernet"))
			Expect(uploader).To(BeNil())
		})
	})

	Describe("GetURL", func() {
		It("Works", func() {
			uploader, err := rdd.NewUploader(makeTempConfig())
			Expect(err).To(Succeed())
			url := uploader.GetURL()
			Expect(url).To(HavePrefix("file:///"))
		})
	})

	testFilePath, _ := rdd.AbsPath("./test_datasets/omop_52_csv/person.csv")
	testFileName := filepath.Base(testFilePath)
	testFileContent := getFileContent(testFilePath)

	Describe("UploadFile", func() {
		It("Works", func() {
			config := makeTempConfig()
			uploader, err := rdd.NewUploader(config)
			Expect(err).To(Succeed())

			file := rdd.File{
				Name:     testFileName,
				FullPath: testFilePath,
			}

			err = uploader.UploadFile(&file)
			Expect(err).To(Succeed())

			filePath := findFile(config.Storage["path"], testFileName)
			Expect(filePath).To(Not(BeEmpty()))
			defer os.Remove(filePath)

			fileContent := getFileContent(filePath)
			Expect(fileContent).To(Equal(testFileContent))
		})

		It("Handles missing files", func() {
			config := makeTempConfig()
			uploader, err := rdd.NewUploader(config)
			Expect(err).To(Succeed())

			file := rdd.File{
				Name:     testFileName,
				FullPath: "./doesntexist",
			}

			err = uploader.UploadFile(&file)
			Expect(err).To(Not(Succeed()))
		})
	})

	Describe("UploadFiles", func() {
		It("Works", func() {
			config := makeTempConfig()
			uploader, err := rdd.NewUploader(config)
			Expect(err).To(Succeed())

			files := []rdd.File{
				{
					Name:     testFileName,
					FullPath: testFilePath,
				},
			}

			err = uploader.UploadFiles(files)
			Expect(err).To(Succeed())

			filePath := findFile(config.Storage["path"], testFileName)
			Expect(filePath).To(Not(BeEmpty()))
			defer os.Remove(filePath)

			fileContent := getFileContent(filePath)
			Expect(fileContent).To(Equal(testFileContent))
		})

		It("Handles missing files", func() {
			config := makeTempConfig()
			uploader, err := rdd.NewUploader(config)
			Expect(err).To(Succeed())

			files := []rdd.File{
				{
					Name:     testFileName,
					FullPath: "./doesntexist",
				},
			}

			err = uploader.UploadFiles(files)
			Expect(err).To(Not(Succeed()))
		})
	})

	Describe("UploadContent", func() {
		It("Works", func() {
			content := "foobar"
			config := makeTempConfig()
			uploader, err := rdd.NewUploader(config)
			Expect(err).To(Succeed())

			err = uploader.UploadContent("somename", []byte(content))
			Expect(err).To(Succeed())

			filePath := findFile(config.Storage["path"], "somename")
			Expect(filePath).To(Not(BeEmpty()))
			defer os.Remove(filePath)

			fileContent := getFileContent(filePath)
			Expect(fileContent).To(Equal(content))
		})
	})
})
