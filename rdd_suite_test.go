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
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestRdd(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Rex Deliver Dataset")
}

func makeTempFile(content []byte) *os.File {
	tmp, _ := ioutil.TempFile("", "rdd_test")
	tmp.Write(content)
	tmp.Close()
	return tmp
}

func tmpdir() string {
	dir, _ := ioutil.TempDir("", "rdd")
	return dir
}
