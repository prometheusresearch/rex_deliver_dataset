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
	"encoding/json"
)

type ManifestFile struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	Sha512 string `json:"sha512"`
}

type Manifest struct {
	DateCreated string         `json:"date_created"`
	DatasetType string         `json:"dataset_type"`
	Generator   string         `json:"generator"`
	Files       []ManifestFile `json:"files"`
}

func CreateManifest(config Configuration, files []File) Manifest {
	mfiles := make([]ManifestFile, len(files))
	for idx, file := range files {
		mfiles[idx] = ManifestFile{
			Name:   file.Name,
			Size:   file.Size,
			Sha512: file.Hash,
		}
	}
	return Manifest{
		DateCreated: TimeAsISO8601(config.ExecutionTime),
		DatasetType: config.DatasetType,
		Files:       mfiles,
	}
}

func (manifest Manifest) ToJSON() ([]byte, error) {
	return json.Marshal(manifest)
}
