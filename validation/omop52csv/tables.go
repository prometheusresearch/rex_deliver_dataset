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
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type fieldValidator func(string) string

type omopTable map[string]fieldValidator

func text(required bool, maxLength uint) fieldValidator {
	return func(value string) string {
		if value == "" {
			if required {
				return "A value is required"
			}
			return ""
		}

		if maxLength > 0 && uint(len(value)) > maxLength {
			return fmt.Sprintf(
				"Value cannot be longer than %d characters",
				maxLength,
			)
		}
		if !utf8.ValidString(value) {
			return fmt.Sprint("Invalid character encoding",
				", allowed encodings are ASCII and UTF-8")
		}
		return ""
	}
}

func integer(required bool) fieldValidator {
	return func(value string) string {
		if value == "" {
			if required {
				return "A value is required"
			}
			return ""
		}

		_, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fmt.Sprintf("\"%s\" is not an integer", value)
		}

		return ""
	}
}

func float(required bool) fieldValidator {
	return func(value string) string {
		if value == "" {
			if required {
				return "A value is required"
			}
			return ""
		}

		_, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return fmt.Sprintf("\"%s\" is not a decimal", value)
		}

		return ""
	}
}

func date(required bool) fieldValidator {
	return func(value string) string {
		if value == "" {
			if required {
				return "A value is required"
			}
			return ""
		}

		_, err := time.Parse("2006-01-02", value)
		if err != nil {
			return fmt.Sprintf("\"%s\" is not a date", value)
		}

		return ""
	}
}

var datetimePatterns = []string{
	"2006-01-02T15:04:05",
	"2006-01-02T15:04:05Z07:00",
}

func datetime(required bool) fieldValidator {
	return func(value string) string {
		if value == "" {
			if required {
				return "A value is required"
			}
			return ""
		}

		for _, pattern := range datetimePatterns {
			_, err := time.Parse(pattern, value)
			if err == nil {
				return ""
			}
		}

		return fmt.Sprintf("\"%s\" is not a datetime", value)
	}
}

var (
	tableDefinitions = map[string]omopTable{
		"CDM_SOURCE": {
			"CDM_SOURCE_NAME":                text(true, 255),
			"CDM_SOURCE_ABBREVIATION":        text(false, 25),
			"CDM_HOLDER":                     text(false, 255),
			"SOURCE_DESCRIPTION":             text(false, 0),
			"SOURCE_DOCUMENTATION_REFERENCE": text(false, 255),
			"CDM_ETL_REFERENCE":              text(false, 255),
			"SOURCE_RELEASE_DATE":            date(false),
			"CDM_RELEASE_DATE":               date(false),
			"CDM_VERSION":                    text(false, 10),
			"VOCABULARY_VERSION":             text(false, 25),
		},

		"PERSON": {
			"PERSON_ID":                   integer(true),
			"GENDER_CONCEPT_ID":           integer(true),
			"YEAR_OF_BIRTH":               integer(true),
			"MONTH_OF_BIRTH":              integer(false),
			"DAY_OF_BIRTH":                integer(false),
			"BIRTH_DATETIME":              datetime(false),
			"RACE_CONCEPT_ID":             integer(true),
			"ETHNICITY_CONCEPT_ID":        integer(true),
			"LOCATION_ID":                 integer(false),
			"PROVIDER_ID":                 integer(false),
			"CARE_SITE_ID":                integer(false),
			"PERSON_SOURCE_VALUE":         text(false, 50),
			"GENDER_SOURCE_VALUE":         text(false, 50),
			"GENDER_SOURCE_CONCEPT_ID":    integer(false),
			"RACE_SOURCE_VALUE":           text(false, 50),
			"RACE_SOURCE_CONCEPT_ID":      integer(false),
			"ETHNICITY_SOURCE_VALUE":      text(false, 50),
			"ETHNICITY_SOURCE_CONCEPT_ID": integer(false),
		},

		"OBSERVATION_PERIOD": {
			"OBSERVATION_PERIOD_ID":         integer(true),
			"PERSON_ID":                     integer(true),
			"OBSERVATION_PERIOD_START_DATE": date(true),
			"OBSERVATION_PERIOD_END_DATE":   date(true),
			"PERIOD_TYPE_CONCEPT_ID":        integer(false),
		},

		"SPECIMEN": {
			"SPECIMEN_ID":                 integer(true),
			"PERSON_ID":                   integer(true),
			"SPECIMEN_CONCEPT_ID":         integer(true),
			"SPECIMEN_TYPE_CONCEPT_ID":    integer(true),
			"SPECIMEN_DATE":               date(false),
			"SPECIMEN_DATETIME":           datetime(false),
			"QUANTITY":                    float(false),
			"UNIT_CONCEPT_ID":             integer(false),
			"ANATOMIC_SITE_CONCEPT_ID":    integer(false),
			"DISEASE_STATUS_CONCEPT_ID":   integer(false),
			"SPECIMEN_SOURCE_ID":          text(false, 50),
			"SPECIMEN_SOURCE_VALUE":       text(false, 50),
			"UNIT_SOURCE_VALUE":           text(false, 50),
			"ANATOMIC_SITE_SOURCE_VALUE":  text(false, 50),
			"DISEASE_STATUS_SOURCE_VALUE": text(false, 50),
		},

		"DEATH": {
			"PERSON_ID":               integer(true),
			"DEATH_DATE":              date(true),
			"DEATH_DATETIME":          datetime(false),
			"DEATH_TYPE_CONCEPT_ID":   integer(true),
			"CAUSE_CONCEPT_ID":        integer(false),
			"CAUSE_SOURCE_VALUE":      text(false, 50),
			"CAUSE_SOURCE_CONCEPT_ID": integer(false),
		},

		"VISIT_OCCURRENCE": {
			"VISIT_OCCURRENCE_ID":           integer(true),
			"PERSON_ID":                     integer(true),
			"VISIT_CONCEPT_ID":              integer(true),
			"VISIT_START_DATE":              date(true),
			"VISIT_START_DATETIME":          datetime(false),
			"VISIT_END_DATE":                date(true),
			"VISIT_END_DATETIME":            datetime(false),
			"VISIT_TYPE_CONCEPT_ID":         integer(true),
			"PROVIDER_ID":                   integer(false),
			"CARE_SITE_ID":                  integer(false),
			"VISIT_SOURCE_VALUE":            text(false, 50),
			"VISIT_SOURCE_CONCEPT_ID":       integer(false),
			"ADMITTING_SOURCE_CONCEPT_ID":   integer(false),
			"ADMITTING_SOURCE_VALUE":        text(false, 50),
			"DISCHARGE_TO_CONCEPT_ID":       integer(false),
			"DISCHARGE_TO_SOURCE_VALUE":     text(false, 50),
			"PRECEDING_VISIT_OCCURRENCE_ID": integer(false),
		},

		"PROCEDURE_OCCURRENCE": {
			"PROCEDURE_OCCURRENCE_ID":     integer(true),
			"PERSON_ID":                   integer(true),
			"PROCEDURE_CONCEPT_ID":        integer(true),
			"PROCEDURE_DATE":              date(true),
			"PROCEDURE_DATETIME":          datetime(false),
			"PROCEDURE_TYPE_CONCEPT_ID":   integer(true),
			"MODIFIER_CONCEPT_ID":         integer(false),
			"QUANTITY":                    integer(false),
			"PROVIDER_ID":                 integer(false),
			"VISIT_OCCURRENCE_ID":         integer(false),
			"PROCEDURE_SOURCE_VALUE":      text(false, 50),
			"PROCEDURE_SOURCE_CONCEPT_ID": integer(false),
			"QUALIFIER_SOURCE_VALUE":      text(false, 50),
		},

		"DRUG_EXPOSURE": {
			"DRUG_EXPOSURE_ID":             integer(true),
			"PERSON_ID":                    integer(true),
			"DRUG_CONCEPT_ID":              integer(true),
			"DRUG_EXPOSURE_START_DATE":     date(true),
			"DRUG_EXPOSURE_START_DATETIME": datetime(true),
			"DRUG_EXPOSURE_END_DATE":       date(true),
			"DRUG_EXPOSURE_END_DATETIME":   datetime(false),
			"VERBATIM_END_DATE":            date(false),
			"DRUG_TYPE_CONCEPT_ID":         integer(true),
			"STOP_REASON":                  text(false, 20),
			"REFILLS":                      integer(false),
			"QUANTITY":                     float(false),
			"DAYS_SUPPLY":                  integer(false),
			"SIG":                          text(false, 0),
			"ROUTE_CONCEPT_ID":             integer(false),
			"LOT_NUMBER":                   text(false, 50),
			"PROVIDER_ID":                  integer(false),
			"VISIT_OCCURRENCE_ID":          integer(false),
			"DRUG_SOURCE_VALUE":            text(false, 50),
			"DRUG_SOURCE_CONCEPT_ID":       integer(false),
			"ROUTE_SOURCE_VALUE":           text(false, 50),
			"DOSE_UNIT_SOURCE_VALUE":       text(false, 50),
		},

		"DEVICE_EXPOSURE": {
			"DEVICE_EXPOSURE_ID":             integer(true),
			"PERSON_ID":                      integer(true),
			"DEVICE_CONCEPT_ID":              integer(true),
			"DEVICE_EXPOSURE_START_DATE":     date(true),
			"DEVICE_EXPOSURE_START_DATETIME": datetime(false),
			"DEVICE_EXPOSURE_END_DATE":       date(false),
			"DEVICE_EXPOSURE_END_DATETIME":   datetime(false),
			"DEVICE_TYPE_CONCEPT_ID":         integer(true),
			"UNIQUE_DEVICE_ID":               text(false, 50),
			"QUANTITY":                       integer(false),
			"PROVIDER_ID":                    integer(false),
			"VISIT_OCCURRENCE_ID":            integer(false),
			"DEVICE_SOURCE_VALUE":            text(false, 100),
			"DEVICE_SOURCE_CONCEPT_ID":       integer(false),
		},

		"CONDITION_OCCURRENCE": {
			"CONDITION_OCCURRENCE_ID":       integer(true),
			"PERSON_ID":                     integer(true),
			"CONDITION_CONCEPT_ID":          integer(true),
			"CONDITION_START_DATE":          date(true),
			"CONDITION_START_DATETIME":      datetime(true),
			"CONDITION_END_DATE":            date(false),
			"CONDITION_END_DATETIME":        datetime(false),
			"CONDITION_TYPE_CONCEPT_ID":     integer(true),
			"STOP_REASON":                   text(false, 20),
			"PROVIDER_ID":                   integer(false),
			"VISIT_OCCURRENCE_ID":           integer(false),
			"CONDITION_SOURCE_VALUE":        text(false, 50),
			"CONDITION_SOURCE_CONCEPT_ID":   integer(false),
			"CONDITION_STATUS_SOURCE_VALUE": text(false, 50),
			"CONDITION_STATUS_CONCEPT_ID":   integer(false),
		},

		"MEASUREMENT": {
			"MEASUREMENT_ID":                integer(true),
			"PERSON_ID":                     integer(true),
			"MEASUREMENT_CONCEPT_ID":        integer(true),
			"MEASUREMENT_DATE":              date(true),
			"MEASUREMENT_DATETIME":          datetime(false),
			"MEASUREMENT_TYPE_CONCEPT_ID":   integer(true),
			"OPERATOR_CONCEPT_ID":           integer(false),
			"VALUE_AS_NUMBER":               float(false),
			"VALUE_AS_CONCEPT_ID":           integer(false),
			"UNIT_CONCEPT_ID":               integer(false),
			"RANGE_LOW":                     float(false),
			"RANGE_HIGH":                    float(false),
			"PROVIDER_ID":                   integer(false),
			"VISIT_OCCURRENCE_ID":           integer(false),
			"MEASUREMENT_SOURCE_VALUE":      text(false, 50),
			"MEASUREMENT_SOURCE_CONCEPT_ID": integer(false),
			"UNIT_SOURCE_VALUE":             text(false, 50),
			"VALUE_SOURCE_VALUE":            text(false, 50),
		},

		"NOTE": {
			"NOTE_ID":               integer(true),
			"PERSON_ID":             integer(true),
			"NOTE_DATE":             date(true),
			"NOTE_DATETIME":         datetime(false),
			"NOTE_TYPE_CONCEPT_ID":  integer(true),
			"NOTE_CLASS_CONCEPT_ID": integer(true),
			"NOTE_TITLE":            text(false, 250),
			"NOTE_TEXT":             text(true, 0),
			"ENCODING_CONCEPT_ID":   integer(true),
			"LANGUAGE_CONCEPT_ID":   integer(true),
			"PROVIDER_ID":           integer(false),
			"VISIT_OCCURRENCE_ID":   integer(false),
			"NOTE_SOURCE_VALUE":     text(false, 50),
		},

		"NOTE_NLP": {
			"NOTE_NLP_ID":                integer(true),
			"NOTE_ID":                    integer(true),
			"SECTION_CONCEPT_ID":         integer(false),
			"SNIPPET":                    text(false, 250),
			"OFFSET":                     text(false, 250),
			"LEXICAL_VARIANT":            text(true, 250),
			"NOTE_NLP_CONCEPT_ID":        integer(false),
			"NOTE_NLP_SOURCE_CONCEPT_ID": integer(false),
			"NLP_SYSTEM":                 text(false, 250),
			"NLP_DATE":                   date(true),
			"NLP_DATETIME":               datetime(false),
			"TERM_EXISTS":                text(false, 1),
			"TERM_TEMPORAL":              text(false, 50),
			"TERM_MODIFIERS":             text(false, 2000),
		},

		"OBSERVATION": {
			"OBSERVATION_ID":                integer(true),
			"PERSON_ID":                     integer(true),
			"OBSERVATION_CONCEPT_ID":        integer(true),
			"OBSERVATION_DATE":              date(true),
			"OBSERVATION_DATETIME":          datetime(false),
			"OBSERVATION_TYPE_CONCEPT_ID":   integer(true),
			"VALUE_AS_NUMBER":               float(false),
			"VALUE_AS_STRING":               text(false, 60),
			"VALUE_AS_CONCEPT_ID":           integer(false),
			"QUALIFIER_CONCEPT_ID":          integer(false),
			"UNIT_CONCEPT_ID":               integer(false),
			"PROVIDER_ID":                   integer(false),
			"VISIT_OCCURRENCE_ID":           integer(false),
			"OBSERVATION_SOURCE_VALUE":      text(false, 50),
			"OBSERVATION_SOURCE_CONCEPT_ID": integer(false),
			"UNIT_SOURCE_VALUE":             text(false, 50),
			"QUALIFIER_SOURCE_VALUE":        text(false, 50),
		},

		"FACT_RELATIONSHIP": {
			"DOMAIN_CONCEPT_ID_1":     integer(true),
			"FACT_ID_1":               integer(true),
			"DOMAIN_CONCEPT_ID_2":     integer(true),
			"FACT_ID_2":               integer(true),
			"RELATIONSHIP_CONCEPT_ID": integer(true),
		},

		"LOCATION": {
			"LOCATION_ID":           integer(true),
			"ADDRESS_1":             text(false, 50),
			"ADDRESS_2":             text(false, 50),
			"CITY":                  text(false, 50),
			"STATE":                 text(false, 2),
			"ZIP":                   text(false, 9),
			"COUNTY":                text(false, 20),
			"LOCATION_SOURCE_VALUE": text(false, 50),
		},

		"CARE_SITE": {
			"CARE_SITE_ID":                  integer(true),
			"CARE_SITE_NAME":                text(false, 255),
			"PLACE_OF_SERVICE_CONCEPT_ID":   integer(false),
			"LOCATION_ID":                   integer(false),
			"CARE_SITE_SOURCE_VALUE":        text(false, 50),
			"PLACE_OF_SERVICE_SOURCE_VALUE": text(false, 50),
		},

		"PROVIDER": {
			"PROVIDER_ID":                 integer(true),
			"PROVIDER_NAME":               text(false, 255),
			"NPI":                         text(false, 20),
			"DEA":                         text(false, 20),
			"SPECIALTY_CONCEPT_ID":        integer(false),
			"CARE_SITE_ID":                integer(false),
			"YEAR_OF_BIRTH":               integer(false),
			"GENDER_CONCEPT_ID":           integer(false),
			"PROVIDER_SOURCE_VALUE":       text(false, 50),
			"SPECIALTY_SOURCE_VALUE":      text(false, 50),
			"SPECIALTY_SOURCE_CONCEPT_ID": integer(false),
			"GENDER_SOURCE_VALUE":         text(false, 50),
			"GENDER_SOURCE_CONCEPT_ID":    integer(false),
		},

		"PAYER_PLAN_PERIOD": {
			"PAYER_PLAN_PERIOD_ID":         integer(true),
			"PERSON_ID":                    integer(true),
			"PAYER_PLAN_PERIOD_START_DATE": date(true),
			"PAYER_PLAN_PERIOD_END_DATE":   date(true),
			"PAYER_SOURCE_VALUE":           text(false, 50),
			"PLAN_SOURCE_VALUE":            text(false, 50),
			"FAMILY_SOURCE_VALUE":          text(false, 50),
		},

		"COST": {
			"COST_ID":                  integer(true),
			"COST_EVENT_ID":            integer(true),
			"COST_DOMAIN_ID":           text(true, 20),
			"COST_TYPE_CONCEPT_ID":     integer(true),
			"CURRENCY_CONCEPT_ID":      integer(false),
			"TOTAL_CHARGE":             float(false),
			"TOTAL_COST":               float(false),
			"TOTAL_PAID":               float(false),
			"PAID_BY_PAYER":            float(false),
			"PAID_BY_PATIENT":          float(false),
			"PAID_PATIENT_COPAY":       float(false),
			"PAID_PATIENT_COINSURANCE": float(false),
			"PAID_PATIENT_DEDUCTIBLE":  float(false),
			"PAID_BY_PRIMARY":          float(false),
			"PAID_INGREDIENT_COST":     float(false),
			"PAID_DISPENSING_FEE":      float(false),
			"PAYER_PLAN_PERIOD_ID":     integer(false),
			"AMOUNT_ALLOWED":           float(false),
			"REVENUE_CODE_CONCEPT_ID":  integer(false),
			"REVEUE_CODE_SOURCE_VALUE": text(false, 50),
			"DRG_CONCEPT_ID":           integer(false),
			"DRG_SOURCE_VALUE":         text(false, 3),
		},

		"COHORT": {
			"COHORT_DEFINITION_ID": integer(true),
			"SUBJECT_ID":           integer(true),
			"COHORT_START_DATE":    date(true),
			"COHORT_END_DATE":      date(true),
		},

		"COHORT_ATTRIBUTE": {
			"COHORT_DEFINITION_ID":    integer(true),
			"COHORT_START_DATE":       date(true),
			"COHORT_END_DATE":         date(true),
			"SUBJECT_ID":              integer(true),
			"ATTRIBUTE_DEFINITION_ID": integer(true),
			"VALUE_AS_NUMBER":         float(false),
			"VALUE_AS_CONCEPT_ID":     integer(false),
		},

		"DRUG_ERA": {
			"DRUG_ERA_ID":         integer(true),
			"PERSON_ID":           integer(true),
			"DRUG_CONCEPT_ID":     integer(true),
			"DRUG_ERA_START_DATE": date(true),
			"DRUG_ERA_END_DATE":   date(true),
			"DRUG_EXPOSURE_COUNT": integer(false),
			"GAP_DAYS":            integer(false),
		},

		"DOSE_ERA": {
			"DOSE_ERA_ID":         integer(true),
			"PERSON_ID":           integer(true),
			"DRUG_CONCEPT_ID":     integer(true),
			"UNIT_CONCEPT_ID":     integer(true),
			"DOSE_VALUE":          float(true),
			"DOSE_ERA_START_DATE": date(true),
			"DOSE_ERA_END_DATE":   date(true),
		},

		"CONDITION_ERA": {
			"CONDITION_ERA_ID":           integer(true),
			"PERSON_ID":                  integer(true),
			"CONDITION_CONCEPT_ID":       integer(true),
			"CONDITION_ERA_START_DATE":   date(true),
			"CONDITION_ERA_END_DATE":     date(true),
			"CONDITION_OCCURRENCE_COUNT": integer(false),
		},
	}
)

var primaryKeyDefinitions = map[string]string{
	"CDM_SOURCE": "CDM_SOURCE_NAME",
	"PERSON": "PERSON_ID",
	"OBSERVATION_PERIOD": "OBSERVATION_PERIOD_ID",
	"SPECIMEN": "SPECIMEN_ID",
	"DEATH": "",
	"VISIT_OCCURRENCE": "VISIT_OCCURRENCE_ID",
	"PROCEDURE_OCCURRENCE": "PROCEDURE_OCCURRENCE_ID",
	"DRUG_EXPOSURE": "DRUG_EXPOSURE_ID",
	"DEVICE_EXPOSURE": "DEVICE_EXPOSURE_ID",
	"CONDITION_OCCURRENCE": "CONDITION_OCCURRENCE_ID",
	"MEASUREMENT": "MEASUREMENT_ID",
	"NOTE": "NOTE_ID",
	"NOTE_NLP": "NOTE_NLP_ID",
	"OBSERVATION": "OBSERVATION_ID",
	"FACT_RELATIONSHIP": "",
	"LOCATION": "LOCATION_ID",
	"CARE_SITE": "CARE_SITE_ID",
	"PROVIDER": "PROVIDER_ID",
	"PAYER_PLAN_PERIOD": "PAYER_PLAN_PERIOD_ID",
	"COST": "COST_ID",
	"COHORT": "COHORT_DEFINITION_ID",
	"COHORT_ATTRIBUTE": "ATTRIBUTE_DEFINITION_ID",
	"DRUG_ERA": "DRUG_ERA_ID",
	"DOSE_ERA": "DOSE_ERA_ID",
	"CONDITION_ERA": "CONDITION_ERA_ID",
}

func getTableName(name string) (string) {
	baseName := filepath.Base(name)
	ext := filepath.Ext(baseName)
	return strings.ToUpper(baseName[:len(baseName)-len(ext)])
}

func getTableDefinitionForFile(name string) (string, omopTable) {
	tableName := getTableName(name)
	return tableName, tableDefinitions[tableName]
}
//revive:disable
func getPrimaryKeyForFile(name string) (string){
	tableName := getTableName(name)
	return primaryKeyDefinitions[tableName]
}
