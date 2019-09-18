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
	"crypto/sha512"
	"encoding/hex"
	"hash"
	"io"
	"os"
)

type FileReader struct {
	io.ReadCloser
	hasher hash.Hash
}

func CreateFileReader(path string) (*FileReader, error) {
	baseReader, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &FileReader{baseReader, sha512.New()}, nil
}

func (fileReader *FileReader) Read(p []byte) (int, error) {
	n, err := fileReader.ReadCloser.Read(p)
	if n > 0 {
		_, err = fileReader.hasher.Write(p[:n])
	}
	return n, err
}

func (fileReader *FileReader) GetHash() string {
	return hex.EncodeToString(fileReader.hasher.Sum(nil))
}
