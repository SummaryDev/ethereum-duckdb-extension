# Ethereum ABI DuckDB extension

This extension, **abi**, allows you to parse EVM events and transaction payloads encoded with ABI.

## Building
To build the extension:
```sh
make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/abi/abi.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `abi.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`.

Now we can use the features from the extension directly in DuckDB.
```
D select to_uint256(2, '0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff');
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│   to_uint256(2, '0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff')    │
│                                         uint64[]                                         │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│ [18446744073709551615, 18446744073709551615, 18446744073709551615, 18446744073709551615] │
└──────────────────────────────────────────────────────────────────────────────────────────┘

D  select blob_to_uint256(0, '\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff'::blob);
┌──────────────────────────────────────────────────────────────────────────────┐
│ blob_to_uint256(0, '\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x…  │
│                                   uint64[]                                   │
├──────────────────────────────────────────────────────────────────────────────┤
│ [18446744073709551615, 18446744073709551615, 18446744073709551615, 1844674…  │
└──────────────────────────────────────────────────────────────────────────────┘
```
## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

### Installing the deployed binaries
To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```
Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:
```sql
INSTALL abi
LOAD abi
```

---

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.