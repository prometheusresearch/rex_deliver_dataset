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

package rexdeliverdataset

import (
	"bytes"
	"fmt"
	"io"
	"path/filepath"

	"github.com/c2fo/vfs/v5"
	gcs "github.com/c2fo/vfs/v5/backend/gs"
	local "github.com/c2fo/vfs/v5/backend/os"
	s3 "github.com/c2fo/vfs/v5/backend/s3"
)

type internalUploader struct {
	location vfs.Location
}

type Uploader interface {
	UploadFile(file *File) error
	UploadFiles(files []File) error
	UploadContent(name string, content []byte) error
	GetURL() string
}

func NewUploader(config Configuration) (Uploader, error) {
	location, err := getLocation(config)
	if err != nil {
		return nil, err
	}
	return internalUploader{
		location: location,
	}, nil
}

func (ul internalUploader) UploadFile(file *File) error {
	reader, err := CreateFileReader(file.FullPath)
	if err != nil {
		return err
	}

	cfile, err := ul.location.NewFile(file.Name)
	if err != nil {
		return err
	}

	_, err = io.Copy(cfile, reader)
	if err != nil {
		return err
	}
	err = cfile.Close()
	if err != nil {
		return err
	}

	file.Hash = reader.GetHash()
	return nil
}

func (ul internalUploader) UploadContent(name string, content []byte) error {
	cfile, err := ul.location.NewFile(name)
	if err != nil {
		return err
	}

	reader := bytes.NewReader(content)
	_, err = io.Copy(cfile, reader)
	if err != nil {
		return err
	}
	err = cfile.Close()
	if err != nil {
		return err
	}

	return nil
}

func (ul internalUploader) UploadFiles(files []File) error {
	for _, file := range files {
		err := ul.UploadFile(&file)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ul internalUploader) GetURL() string {
	return string(ul.location.URI())
}

func getLocation(config Configuration) (vfs.Location, error) {
	var fs vfs.FileSystem

	sep := string(filepath.Separator)

	path := config.Storage["path"]
	if path == "" {
		path = sep
	}

	container := config.Storage["container"]

	switch config.Storage["kind"] {
	case "s3":
		sfs := s3.NewFileSystem()
		sfs = sfs.WithOptions(
			s3.Options{
				AccessKeyID:     config.Storage["access_key"],
				SecretAccessKey: config.Storage["secret_key"],
				Region:          config.Storage["region"],
			},
		)
		fs = sfs

	case "gcs":
		sfs := gcs.NewFileSystem()
		sfs = sfs.WithOptions(
			gcs.Options{
				CredentialFile: config.Storage["credentials_json"],
				Scopes:         []string{"ScopeReadWrite"},
			},
		)
		fs = sfs

	case "local":
		fs = &local.FileSystem{}
		container = ""
		path = filepath.Join(
			config.Storage["path"],
			config.Storage["container"],
		)

	default:
		return nil, fmt.Errorf(
			"Unknown storage kind: %s",
			config.Storage["kind"],
		)
	}

	path = filepath.Join(
		path,
		config.ExecutionTime.UTC().Format("20060102150405"),
	) + sep

	return fs.NewLocation(container, path)
}
