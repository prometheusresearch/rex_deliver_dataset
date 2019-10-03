# Dataset Manifest File

The Dataset Manifest is a simple file that documents various metadata about the
dataset being delivered and the files that make it up. These files must adhere
to the following requirements:

* The file name must be `MANIFEST.json`. The name is case-sensitive.
* The contents of the file must be a single [JSON](https://json.org) object.
* The JSON object allows the following properties:
  * date_created
    * An ISO8601-formatted date and time string that specifies when the dataset
      described by the manifest was created.
    * This property is required.
  * dataset_type
    * A string that identifies the type of dataset described by the manifest.
    * This property is required.
    * The following values are permitted:
      * omop-5.2-csv
  * files
    * An array of objects that lists all the files that are a part of the
      dataset.
    * This property is required.
    * Each object in the array allows the following properties:
      * name
        * A string containing the name of the file.
        * This property is required.
        * The name is case-sensitive.
      * size
        * An integer specifying the size of the file in bytes.
        * This property is required.
      * sha512
        * A string containing the hexadecimal-encoded SHA512 hash of the file's
          contents.
        * The string is case-insensitive.
        * This property is required.

An example of a Dataset Manifest is as follows:

```json
{
    "date_created": "2019-05-22T12:34:56Z",
    "dataset_type": "omop-5.2-csv",
    "files": [
        {
            "name": "person.csv",
            "size": 11,
            "sha512": "309ecc489c12d6eb4cc40f50c902f2b4d0ed77ee511a7c7a9bcd3ca86d4cd86f989dd35bc5ff499670da34255b45b0cfd830e81f605dcf7dc5542e93ae9cd76f"
        },
        {
            "name": "specimen.csv",
            "size": 17,
            "sha512": "76cef72e24a58b90331bc9a31e9400c0356d2101b6e3051fe61f1ec4c582d6d7c7f695289d8f4a41288c4af8a2d01d6777bbabd51906508e5132cdf4dbabd567"
        }
    ]
}
```

