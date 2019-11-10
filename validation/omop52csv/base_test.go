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

package omop52csv_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	rdd "github.com/prometheusresearch/rex_deliver_dataset"
	val "github.com/prometheusresearch/rex_deliver_dataset/validation"
	omop "github.com/prometheusresearch/rex_deliver_dataset/validation/omop52csv"
)

var _ = Describe("ValidateOmop52", func() {
	datasetPath, _ := rdd.AbsPath("../../test_datasets/omop_52_csv")
	badDatasetPath, _ := rdd.AbsPath("../../test_datasets/omop_52_csv_bad")

	Describe("File Name Issues", func() {
		It("Checks for subdirectories", func() {
			errors := omop.ValidateOmop52(
				datasetPath,
				[]string{
					"subdir/person.csv",
				},
			)

			Expect(errors.Errors["subdir/person.csv"]).To(ConsistOf(
				val.Error{
					Message: "Files must not be in subdirectories",
					Record:  0,
					Column:  "",
				},
			))
		})

		It("Checks for .csv", func() {
			errors := omop.ValidateOmop52(
				datasetPath,
				[]string{
					"specimen.data",
				},
			)

			Expect(errors.Errors["specimen.data"]).To(ConsistOf(
				val.Error{
					Message: "Files must have a .csv extension",
					Record:  0,
					Column:  "",
				},
			))
		})

		It("Checks if name is an OMOP table", func() {
			errors := omop.ValidateOmop52(
				datasetPath,
				[]string{
					"notreal.csv",
					"person.csv",
				},
			)

			Expect(errors.Errors["notreal.csv"]).To(ConsistOf(
				val.Error{
					Message: "NOTREAL is not an OMOP table name",
					Record:  0,
					Column:  "",
				},
			))
			Expect(errors.Errors["person.csv"]).To(BeEmpty())
		})

		It("Checks for dupe OMOP tables", func() {
			errors := omop.ValidateOmop52(
				datasetPath,
				[]string{
					"PERSON.csv",
					"person.csv",
				},
			)

			if len(errors.Errors["PERSON.csv"]) > 0 {
				Expect(errors.Errors["PERSON.csv"]).To(HaveLen(1))
				Expect(errors.Errors["PERSON.csv"][0].Message).To(HavePrefix("Could not open file"))
			} else {
				Expect(errors.Errors["PERSON.csv"]).To(BeEmpty())
			}
			Expect(errors.Errors["person.csv"]).To(ConsistOf(
				val.Error{
					Message: "Cannot provide multiple files for PERSON table",
					Record:  0,
					Column:  "",
				},
			))
		})

		It("Find multiple issues at a time", func() {
			errors := omop.ValidateOmop52(
				datasetPath,
				[]string{
					"multiple/failures.data",
				},
			)

			Expect(errors.Errors["multiple/failures.data"]).To(ConsistOf(
				val.Error{
					Message: "Files must not be in subdirectories",
					Record:  0,
					Column:  "",
				},
				val.Error{
					Message: "Files must have a .csv extension",
					Record:  0,
					Column:  "",
				},
				val.Error{
					Message: "FAILURES is not an OMOP table name",
					Record:  0,
					Column:  "",
				},
			))
		})

		It("Handles odd file names", func() {
			errors := omop.ValidateOmop52(
				datasetPath,
				[]string{
					".DS_Store",
					".foo.csv",
				},
			)

			Expect(errors.Errors[".DS_Store"]).To(ConsistOf(
				val.Error{
					Message: "Files must have a .csv extension",
					Record:  0,
					Column:  "",
				},
				val.Error{
					Message: ".DS_Store is not an OMOP table name",
					Record:  0,
					Column:  "",
				},
			))

			Expect(errors.Errors[".foo.csv"]).To(ConsistOf(
				val.Error{
					Message: ".FOO is not an OMOP table name",
					Record:  0,
					Column:  "",
				},
			))
		})

		It("Handles missing files", func() {
			errors := omop.ValidateOmop52(
				datasetPath,
				[]string{
					"note_nlp.csv",
				},
			)

			Expect(errors.Errors["note_nlp.csv"]).To(HaveLen(1))
			Expect(errors.Errors["note_nlp.csv"][0].Message).To(HavePrefix("Could not open file"))
			Expect(errors.Errors["note_nlp.csv"][0].Record).To(Equal(uint32(0)))
			Expect(errors.Errors["note_nlp.csv"][0].Column).To(Equal(""))
		})
	})

	Describe("Header Issues", func() {
		It("Finds missing and extra columns", func() {
			errors := omop.ValidateOmop52(
				datasetPath,
				[]string{
					"observation_period.csv",
				},
			)

			Expect(errors.Errors["observation_period.csv"]).To(ConsistOf(
				val.Error{
					Message: "Unknown column: BOGUS_COL1",
					Record:  0,
					Column:  "",
				},
				val.Error{
					Message: "Unknown column: BOGUS_COL2",
					Record:  0,
					Column:  "",
				},
				val.Error{
					Message: "Missing column: OBSERVATION_PERIOD_START_DATE",
					Record:  0,
					Column:  "",
				},
				val.Error{
					Message: "Missing column: OBSERVATION_PERIOD_END_DATE",
					Record:  0,
					Column:  "",
				},
			))
		})

		It("Finds no records at all", func() {
			errors := omop.ValidateOmop52(
				datasetPath,
				[]string{
					"cdm_source.csv",
				},
			)

			Expect(errors.Errors["cdm_source.csv"]).To(ConsistOf(
				val.Error{
					Message: "No column headers found",
					Record:  0,
					Column:  "",
				},
			))
		})
	})

	Describe("Record Issues", func() {
		It("Finds CSV encoding errors", func() {
			errors := omop.ValidateOmop52(
				badDatasetPath,
				[]string{
					"person.csv",
					"note_nlp.csv",
				},
			)

			Expect(errors.Errors["person.csv"]).To(ConsistOf(
				val.Error{
					Message: "wrong number of fields",
					Record:  1,
					Column:  "",
				},
				val.Error{
					Message: "wrong number of fields",
					Record:  2,
					Column:  "",
				},
				val.Error{
					Message: "extraneous or missing \" in quoted-field",
					Record:  3,
					Column:  "",
				},
			))
			Expect(errors.Errors["note_nlp.csv"]).To(ConsistOf(
				val.Error{
					Message: "extraneous or missing \" in quoted-field",
					Record:  0,
					Column:  "",
				},
				val.Error{
					Message: "No column headers found",
					Record:  0,
					Column:  "",
				},
			))
		})

		It("Finds data type errors", func() {
			errors := omop.ValidateOmop52(
				badDatasetPath,
				[]string{
					"note.csv",
					"dose_era.csv",
					"cdm_source.csv",
					"drug_exposure.csv",
				},
			)

			Expect(errors.Errors["note.csv"]).To(ConsistOf(
				val.Error{
					Message: "A value is required",
					Record:  2,
					Column:  "NOTE_TEXT",
				},
			))

			Expect(errors.Errors["dose_era.csv"]).To(ConsistOf(
				val.Error{
					Message: "A value is required",
					Record:  2,
					Column:  "DOSE_VALUE",
				},
				val.Error{
					Message: "\"not-a-float\" is not a decimal",
					Record:  3,
					Column:  "DOSE_VALUE",
				},
			))

			Expect(errors.Errors["cdm_source.csv"]).To(ConsistOf(
				val.Error{
					Message: "Value cannot be longer than 255 characters",
					Record:  3,
					Column:  "CDM_SOURCE_NAME",
				},
				val.Error{
					Message: "Value cannot be longer than 25 characters",
					Record:  3,
					Column:  "CDM_SOURCE_ABBREVIATION",
				},
				val.Error{
					Message: "A value is required",
					Record:  4,
					Column:  "CDM_SOURCE_NAME",
				},
			))

			Expect(errors.Errors["drug_exposure.csv"]).To(ConsistOf(
				val.Error{
					Message: "\"not-an-int\" is not an integer",
					Record:  2,
					Column:  "DRUG_EXPOSURE_ID",
				},
				val.Error{
					Message: "\"not-a-date\" is not a date",
					Record:  2,
					Column:  "DRUG_EXPOSURE_START_DATE",
				},
				val.Error{
					Message: "\"not-a-datetime\" is not a datetime",
					Record:  2,
					Column:  "DRUG_EXPOSURE_START_DATETIME",
				},
				val.Error{
					Message: "\"not-a-datetime\" is not a datetime",
					Record:  2,
					Column:  "DRUG_EXPOSURE_END_DATETIME",
				},
				val.Error{
					Message: "\"not-a-date\" is not a date",
					Record:  2,
					Column:  "VERBATIM_END_DATE",
				},
				val.Error{
					Message: "\"not-a-float\" is not a decimal",
					Record:  2,
					Column:  "QUANTITY",
				},
				val.Error{
					Message: "\"not-an-int\" is not an integer",
					Record:  2,
					Column:  "DRUG_SOURCE_CONCEPT_ID",
				},
				val.Error{
					Message: "A value is required",
					Record:  3,
					Column:  "DRUG_EXPOSURE_ID",
				},
				val.Error{
					Message: "A value is required",
					Record:  3,
					Column:  "DRUG_EXPOSURE_START_DATE",
				},
				val.Error{
					Message: "A value is required",
					Record:  3,
					Column:  "DRUG_EXPOSURE_START_DATETIME",
				},
			))
		})

		It("Handles a variety of datetime formats", func() {
			errors := omop.ValidateOmop52(
				badDatasetPath,
				[]string{
					"death.csv",
				},
			)

			Expect(errors.Errors["death.csv"]).To(BeEmpty())
		})
	})
})
