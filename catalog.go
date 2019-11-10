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
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type File struct {
	Name     string
	FullPath string
	Size     int64
	Hash     string
}

func CatalogDirectory(rootPath string) ([]File, error) {
	var files []File

	info, err := os.Stat(rootPath)
	if err != nil {
		return files, err
	} else if !info.IsDir() {
		return files, fmt.Errorf("%s is not a directory", rootPath)
	}

	err = filepath.Walk(
		rootPath,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.Mode().IsRegular() {
				rel, err := filepath.Rel(rootPath, path)
				if err != nil {
					return err
				}
				files = append(files, File{
					Name:     strings.ReplaceAll(rel, "\\", "/"),
					FullPath: path,
					Size:     info.Size(),
				})
			}
			return nil
		},
	)

	if err != nil {
		return nil, err
	}

	return files, nil
}
