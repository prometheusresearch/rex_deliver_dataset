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

package omop52csv

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	val "github.com/prometheusresearch/rex_deliver_dataset/validation"
)

type recordValidatorField struct {
	Name      string
	Validator fieldValidator
}

type recordValidatorError struct {
	Column string
	Error  string
}

type recordValidator func([]string) []recordValidatorError

func noop(value string) string { // revive:disable:unused-parameter
	return ""
}

func makeRecordValidator(
	definition omopTable,
	headers []string,
) (recordValidator, []string) {
	errors := make([]string, 0)
	validators := make([]recordValidatorField, len(headers))
	foundHeaders := make(map[string]string, len(headers))

	for idx, header := range headers {
		column := strings.ToUpper(header)
		foundHeaders[column] = header
		validators[idx] = recordValidatorField{Name: column}

		validator, ok := definition[column]
		if !ok {
			errors = append(errors, fmt.Sprintf("Unknown column: %s", column))
			validators[idx].Validator = noop
		} else {
			validators[idx].Validator = validator
		}
	}

	for column := range definition {
		_, ok := foundHeaders[column]
		if !ok {
			errors = append(errors, fmt.Sprintf("Missing column: %s", column))
		}
	}

	recValidator := func(record []string) []recordValidatorError {
		recordErrors := make([]recordValidatorError, 0)
		for idx, column := range record {
			recError := validators[idx].Validator(column)
			if recError != "" {
				recordErrors = append(
					recordErrors,
					recordValidatorError{
						Column: validators[idx].Name,
						Error:  recError,
					},
				)
			}
		}
		return recordErrors
	}

	return recValidator, errors
}

func checkFileContents(
	basePath string,
	file string,
	definition omopTable,
	errors val.ErrorCollection,
) {
	fileReader, err := os.Open(filepath.Join(basePath, file))
	if err != nil {
		errors.FileError(file, fmt.Sprintf("Could not open file: %v", err))
		return
	}
	defer fileReader.Close()

	var recValidator recordValidator
	var recNumber uint32
	var headerErrors []string

	seenRecords := make(map[string]bool)

	csvReader := csv.NewReader(fileReader)
	csvReader.ReuseRecord = true

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		recNumber++

		if err != nil {
			// The record is fundamentally broken somehow
			csvErr := strings.SplitAfter(err.Error(), ": ")
			errors.RecordError(file, recNumber-1, csvErr[len(csvErr)-1])

			if recValidator == nil {
				break
			}
		} else if recNumber == 1 {
			// This should be the header record
			recValidator, headerErrors = makeRecordValidator(
				definition,
				record,
			)
			if len(headerErrors) > 0 {
				for _, err := range headerErrors {
					errors.FileError(file, err)
				}

				// The headers are hosed, don't bother with the file content.
				break
			}
		} else {
			// This is a data record
			recErrors := recValidator(record)
			for _, err := range recErrors {
				errors.ValueError(
					file,
					recNumber-1,
					err.Column,
					err.Error,
				)
			}
			_, ok := seenRecords[record[0]]
			if ok {
				errors.RecordError(
					file,
					recNumber-1,
					"Primary key should be unique in CSV file",
				)
			}
			seenRecords[record[0]] = true
		}
	}

	if recValidator == nil {
		errors.FileError(file, "No column headers found")
	}
}

func ValidateOmop52(basePath string, files []string) val.ErrorCollection {
	errors := val.NewErrorCollection()

	seenTables := make(map[string]bool)

	for i := range files {
		name := files[i]

		baseName := filepath.Base(name)
		if name != baseName {
			errors.FileError(name, "Files must not be in subdirectories")
		}
		ext := strings.ToUpper(filepath.Ext(baseName))
		if ext != ".CSV" {
			errors.FileError(name, "Files must have a .csv extension")
		}

		table, tableDefinition := getTableDefinitionForFile(name)
		if tableDefinition == nil {
			if table == "" {
				table = baseName
			}
			errors.FileError(name, "%s is not an OMOP table name", table)
		} else {
			_, ok := seenTables[table]
			if ok {
				errors.FileError(
					name,
					"Cannot provide multiple files for %s table",
					table,
				)
			}
			seenTables[table] = true
		}

		if errors.FileHasErrors(name) {
			continue
		}
		checkFileContents(basePath, name, tableDefinition, errors)
	}

	return errors
}

func init() {
	val.Register("omop:5.2:csv", ValidateOmop52)
}
