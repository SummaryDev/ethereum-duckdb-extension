#define DUCKDB_EXTENSION_MAIN

#include "abi_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"

#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

    /*
    * Functions take input is blob
    */

    constexpr char hexmap[] = {'0', '1', '2', '3', '4', '5', '6', '7',
                               '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    inline std::string BlobToHex(uint16_t pos, const unsigned char *data, uint16_t len) {
        std::string s(len * 2, '0');
        for (int i = 0; i < len; ++i) {
            auto d = data[pos + i];
            auto h1 = hexmap[(d & 0xF0) >> 4];
            auto h2 = hexmap[d & 0x0F];
            s[2 * i] = h1;
            s[2 * i + 1] = h2;
        }
        return s;
    }

//    inline std::string BlobToHex(uint16_t pos, const char* data, uint16_t len) {
//        std::stringstream ss;
//        ss << std::hex;
//
//        for( int i(pos) ; i < pos + len; ++i )
//            ss << std::setw(2) << std::setfill('0') << (int)data[i];
//
//        return ss.str();
//    }

    inline void BlobToAddress(DataChunk &args, ExpressionState &state, Vector &result) {
        auto &pos_vector = args.data[0];
        auto &data_vector = args.data[1];
        BinaryExecutor::Execute<uint16_t, string_t, string_t>(
                pos_vector, data_vector, result, args.size(),
                [&](uint16_t pos, string_t data) {
                    auto s = (const unsigned char*) data.GetDataUnsafe();
                    auto h = "0x" + BlobToHex(pos + 32 - 20, s, 20);
                    return StringVector::AddString(result, h);
                });
    }

    template<class U>
    inline U BlobToInt(uint16_t pos, const unsigned char *s) {
        U val;
        auto size = sizeof(U);
        std::vector<unsigned char> reversed(size);
        auto first = s + pos;
        auto last = first + size;
        std::reverse_copy(first, last, std::begin(reversed));
        val = *reinterpret_cast<U *>(reversed.data());
        return val;
    }

    template<class U>
    inline U PaddedBlobToInt(uint16_t pos, const unsigned char *s) {
        return BlobToInt<U>(pos + 32 - sizeof(U), s);
    }

    inline void BlobToString(DataChunk &args, ExpressionState &state, Vector &result) {
        auto &pos_vector = args.data[0];
        auto &data_vector = args.data[1];
        BinaryExecutor::Execute<uint16_t, string_t, string_t>(
                pos_vector, data_vector, result, args.size(),
                [&](uint16_t pos, string_t data) {
                    auto s = (const unsigned char*) data.GetDataUnsafe();

                    uint16_t location = PaddedBlobToInt<uint16_t>(pos, s);
                    uint16_t size = PaddedBlobToInt<uint16_t>(location, s);

                    std::vector<unsigned char> dest(size);
                    auto first = s + location + 32;
                    auto last = first + size;
                    std::copy(first, last, std::begin(dest));

                    std::string ret(dest.begin(), dest.end());

                    return StringVector::AddString(result, ret);
                });
    }

    inline void BlobToBytes(DataChunk &args, ExpressionState &state, Vector &result) {
        auto &pos_vector = args.data[0];
        auto &data_vector = args.data[1];
        BinaryExecutor::Execute<uint16_t, string_t, string_t>(
                pos_vector, data_vector, result, args.size(),
                [&](uint16_t pos, string_t data) {
                    auto s = (const unsigned char*) data.GetDataUnsafe();

                    uint16_t location = PaddedBlobToInt<uint16_t>(pos, s);
                    uint16_t size = PaddedBlobToInt<uint16_t>(location, s);

                    std::string ret = "0x" + BlobToHex(location + 32, s, size);

                    return StringVector::AddString(result, ret);
                });
    }

    template<class U>
    inline void BlobToInt(DataChunk &args, Vector &result) {
        BinaryExecutor::Execute<uint16_t, string_t, U>(
                args.data[0], args.data[1], result, args.size(),
                [&](uint16_t pos, string_t data) {
                    return PaddedBlobToInt<U>(pos, (const unsigned char*) data.GetDataUnsafe());
                });
    }

    inline void BlobToUint8(DataChunk &args, ExpressionState &state, Vector &result) {
        BlobToInt<uint8_t>(args, result);
    }

    inline void BlobToUint16(DataChunk &args, ExpressionState &state, Vector &result) {
        BlobToInt<uint16_t>(args, result);
    }

    inline void BlobToUint32(DataChunk &args, ExpressionState &state, Vector &result) {
        BlobToInt<uint32_t>(args, result);
    }

    inline void BlobToUint64(DataChunk &args, ExpressionState &state, Vector &result) {
        BlobToInt<uint64_t>(args, result);
    }

    inline void BlobToUint128(DataChunk &args, ExpressionState &state, Vector &result) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);

        auto result_data = ListVector::GetData(result);

        for (idx_t i = 0; i < args.size(); i++) {
            result_data[i].offset = ListVector::GetListSize(result);
            result_data[i].length = 2;

            auto pos = args.GetValue(0, i).GetValue<uint16_t>();
            auto data = args.GetValue(1, i).GetValueUnsafe<string>();
            const uint8_t size = 8;

            auto s = (const unsigned char*) data.data();

            auto val0 = BlobToInt<uint64_t>(pos, s);
            auto val1 = BlobToInt<uint64_t>(pos + size, s);

            ListVector::PushBack(result, Value::UBIGINT(val0));
            ListVector::PushBack(result, Value::UBIGINT(val1));
        }

        result.Verify(args.size());
    }

    inline void BlobToUint256(DataChunk &args, ExpressionState &state, Vector &result) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);

        auto result_data = ListVector::GetData(result);

        for (idx_t i = 0; i < args.size(); i++) {
            result_data[i].offset = ListVector::GetListSize(result);
            result_data[i].length = 4;

            auto pos = args.GetValue(0, i).GetValue<uint16_t>();
            auto data = args.GetValue(1, i).GetValueUnsafe<string>();
            const uint8_t size = 8;

            auto s = (const unsigned char*) data.data();

            auto val0 = BlobToInt<uint64_t>(pos, s);
            auto val1 = BlobToInt<uint64_t>(pos + size, s);
            auto val2 = BlobToInt<uint64_t>(pos + size * 2, s);
            auto val3 = BlobToInt<uint64_t>(pos + size * 3, s);

            ListVector::PushBack(result, Value::UBIGINT(val0));
            ListVector::PushBack(result, Value::UBIGINT(val1));
            ListVector::PushBack(result, Value::UBIGINT(val2));
            ListVector::PushBack(result, Value::UBIGINT(val3));
        }

        result.Verify(args.size());
    }

    inline void BlobToInt8(DataChunk &args, ExpressionState &state, Vector &result) {
        BlobToInt<int8_t>(args, result);
    }

    inline void BlobToInt16(DataChunk &args, ExpressionState &state, Vector &result) {
        BlobToInt<int16_t>(args, result);
    }

    inline void BlobToInt32(DataChunk &args, ExpressionState &state, Vector &result) {
        BlobToInt<int32_t>(args, result);
    }

    inline void BlobToInt64(DataChunk &args, ExpressionState &state, Vector &result) {
        BlobToInt<int64_t>(args, result);
    }

    /*
     * Functions take input is hex
     */

    inline std::string Substr(uint16_t pos, string_t data, uint8_t size) {
        auto s = data.GetString();
        auto p = pos + 64 - size;
        auto r = s.substr(p, size);
        return r;
    }

    inline void ToPart(DataChunk &args, ExpressionState &state, Vector &result) {
        auto &pos_vector = args.data[0];
        auto &data_vector = args.data[1];
        auto &size_vector = args.data[2];
        TernaryExecutor::Execute<uint16_t, string_t, uint8_t, string_t>(
                pos_vector, data_vector, size_vector, result, args.size(),
                [&](uint16_t pos, string_t data, uint8_t size) {
                    auto s = Substr(pos, data, size);
                    return StringVector::AddString(result, s);
                });
    }

    inline void ToAddress(DataChunk &args, ExpressionState &state, Vector &result) {
        auto &pos_vector = args.data[0];
        auto &data_vector = args.data[1];
        BinaryExecutor::Execute<uint16_t, string_t, string_t>(
                pos_vector, data_vector, result, args.size(),
                [&](uint16_t pos, string_t data) {
                    auto s = Substr(pos, data, 20);
                    auto r = "0x" + s;
                    return StringVector::AddString(result, r);
                });
    }

    inline void ToString(DataChunk &args, ExpressionState &state, Vector &result) {
        auto &pos_vector = args.data[0];
        auto &data_vector = args.data[1];
        BinaryExecutor::Execute<uint16_t, string_t, string_t>(
                pos_vector, data_vector, result, args.size(),
                [&](uint16_t pos, string_t data) {
                    auto s = Substr(pos, data, 20);
                    return StringVector::AddString(result, s);
                });
    }

    template<class U>
    inline void ToUint(DataChunk &args, Vector &result, uint8_t size) {
        BinaryExecutor::Execute<uint16_t, string_t, U>(
                args.data[0], args.data[1], result, args.size(),
                [&](uint16_t pos, string_t data) {
                    auto s = data.GetString().substr(pos, size);
                    U val;
                    val = std::stoull(s, nullptr, 16);
                    return val;
                });
    }

    inline void ToUint8(DataChunk &args, ExpressionState &state, Vector &result) {
        ToUint<uint64_t>(args, result, 2);
    }

    inline void ToUint16(DataChunk &args, ExpressionState &state, Vector &result) {
        ToUint<uint64_t>(args, result, 4);
    }

    inline void ToUint32(DataChunk &args, ExpressionState &state, Vector &result) {
        ToUint<uint64_t>(args, result, 8);
    }

    inline void ToUint64(DataChunk &args, ExpressionState &state, Vector &result) {
        ToUint<uint64_t>(args, result, 16);
    }

    inline void ToUint128(DataChunk &args, ExpressionState &state, Vector &result) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);

        auto result_data = ListVector::GetData(result);

        for (idx_t i = 0; i < args.size(); i++) {
            result_data[i].offset = ListVector::GetListSize(result);
            result_data[i].length = 2;

            auto pos = args.GetValue(0, i).GetValue<uint16_t>();
            auto data = args.GetValue(1, i).GetValue<string>();
            const uint8_t size = 16;

            auto data0 = data.substr(pos, size);
            auto data1 = data.substr(pos + size, size);

            auto val0 = std::stoull(data0, nullptr, 16);
            auto val1 = std::stoull(data1, nullptr, 16);

            ListVector::PushBack(result, Value::UBIGINT(val0));
            ListVector::PushBack(result, Value::UBIGINT(val1));
        }

        result.Verify(args.size());
    }

    inline void ToUint256(DataChunk &args, ExpressionState &state, Vector &result) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);

        auto result_data = ListVector::GetData(result);

        for (idx_t i = 0; i < args.size(); i++) {
            result_data[i].offset = ListVector::GetListSize(result);
            result_data[i].length = 4;

            auto pos = args.GetValue(0, i).GetValue<uint16_t>();
            auto data = args.GetValue(1, i).GetValue<string>();
            const uint8_t size = 16;

            auto data0 = data.substr(pos, size);
            auto data1 = data.substr(pos + size, size);
            auto data2 = data.substr(pos + size * 2, size);
            auto data3 = data.substr(pos + size * 3, size);

            auto val0 = std::stoull(data0, nullptr, 16);
            auto val1 = std::stoull(data1, nullptr, 16);
            auto val2 = std::stoull(data2, nullptr, 16);
            auto val3 = std::stoull(data3, nullptr, 16);

            ListVector::PushBack(result, Value::UBIGINT(val0));
            ListVector::PushBack(result, Value::UBIGINT(val1));
            ListVector::PushBack(result, Value::UBIGINT(val2));
            ListVector::PushBack(result, Value::UBIGINT(val3));
        }

        result.Verify(args.size());
    }

    template<class S, class U>
    inline void ToInt(DataChunk &args, Vector &result, uint8_t size) {
        BinaryExecutor::Execute<uint16_t, string_t, S>(
                args.data[0], args.data[1], result, args.size(),
                [&](uint16_t pos, string_t data) {
                    U two_complement_val;
                    auto s = data.GetString().substr(pos, size);
                    two_complement_val = std::stoull(s, nullptr, 16);

                    U sign_mask = 0x8;

                    // if positive
                    if ((two_complement_val & sign_mask) == 0) {
                        return S(two_complement_val);
                        //  if negative
                    } else {
                        // invert all bits, add one, and make negative
                        return S(-(~two_complement_val + 1));
                    }
                });
    }

    inline void ToInt8(DataChunk &args, ExpressionState &state, Vector &result) {
        ToInt<int8_t, uint8_t>(args, result, 2);
    }

    inline void ToInt16(DataChunk &args, ExpressionState &state, Vector &result) {
        ToInt<int16_t, uint16_t>(args, result, 4);
    }

    inline void ToInt32(DataChunk &args, ExpressionState &state, Vector &result) {
        ToInt<int32_t, uint32_t>(args, result, 8);
    }

    inline void ToInt64(DataChunk &args, ExpressionState &state, Vector &result) {
        ToInt<int64_t, uint64_t>(args, result, 16);
    }

    static void LoadInternal(DatabaseInstance &instance) {
        Connection con(instance);
        con.BeginTransaction();

        auto &catalog = Catalog::GetSystemCatalog(*con.context);

        CreateScalarFunctionInfo abi_blob_to_uint8_fun_info(
                ScalarFunction("blob_to_uint8", {LogicalType::INTEGER, LogicalType::BLOB}, LogicalType::UTINYINT,
                               BlobToUint8));
        abi_blob_to_uint8_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        catalog.CreateFunction(*con.context, &abi_blob_to_uint8_fun_info);

        CreateScalarFunctionInfo abi_blob_to_uint16_fun_info(
                ScalarFunction("blob_to_uint16", {LogicalType::INTEGER, LogicalType::BLOB}, LogicalType::USMALLINT,
                               BlobToUint16));
        abi_blob_to_uint16_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        catalog.CreateFunction(*con.context, &abi_blob_to_uint16_fun_info);

        CreateScalarFunctionInfo abi_blob_to_uint32_fun_info(
                ScalarFunction("blob_to_uint32", {LogicalType::INTEGER, LogicalType::BLOB}, LogicalType::UINTEGER,
                               BlobToUint32));
        abi_blob_to_uint32_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        catalog.CreateFunction(*con.context, &abi_blob_to_uint32_fun_info);

        CreateScalarFunctionInfo abi_blob_to_uint64_fun_info(
                ScalarFunction("blob_to_uint64", {LogicalType::INTEGER, LogicalType::BLOB}, LogicalType::UBIGINT,
                               BlobToUint64));
        abi_blob_to_uint64_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        catalog.CreateFunction(*con.context, &abi_blob_to_uint64_fun_info);

        CreateScalarFunctionInfo abi_blob_to_uint128_fun_info(
                ScalarFunction("blob_to_uint128", {LogicalType::INTEGER, LogicalType::BLOB},
                               LogicalType::LIST(LogicalType::UBIGINT),
                               BlobToUint128));
        abi_blob_to_uint128_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        catalog.CreateFunction(*con.context, &abi_blob_to_uint128_fun_info);

        CreateScalarFunctionInfo abi_blob_to_uint256_fun_info(
                ScalarFunction("blob_to_uint256", {LogicalType::INTEGER, LogicalType::BLOB},
                               LogicalType::LIST(LogicalType::UBIGINT),
                               BlobToUint256));
        abi_blob_to_uint256_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        catalog.CreateFunction(*con.context, &abi_blob_to_uint256_fun_info);

        CreateScalarFunctionInfo abi_blob_to_int8_fun_info(
                ScalarFunction("blob_to_int8", {LogicalType::INTEGER, LogicalType::BLOB}, LogicalType::TINYINT,
                               BlobToInt8));
        abi_blob_to_int8_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        catalog.CreateFunction(*con.context, &abi_blob_to_int8_fun_info);

        CreateScalarFunctionInfo abi_blob_to_int16_fun_info(
                ScalarFunction("blob_to_int16", {LogicalType::INTEGER, LogicalType::BLOB}, LogicalType::SMALLINT,
                               BlobToInt16));
        abi_blob_to_int16_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        catalog.CreateFunction(*con.context, &abi_blob_to_int16_fun_info);

        CreateScalarFunctionInfo abi_blob_to_int32_fun_info(
                ScalarFunction("blob_to_int32", {LogicalType::INTEGER, LogicalType::BLOB}, LogicalType::INTEGER,
                               BlobToInt32));
        abi_blob_to_int32_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        catalog.CreateFunction(*con.context, &abi_blob_to_int32_fun_info);

        CreateScalarFunctionInfo abi_blob_to_int64_fun_info(
                ScalarFunction("blob_to_int64", {LogicalType::INTEGER, LogicalType::BLOB}, LogicalType::BIGINT,
                               BlobToInt64));
        abi_blob_to_int64_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        catalog.CreateFunction(*con.context, &abi_blob_to_int64_fun_info);

        CreateScalarFunctionInfo abi_to_uint8_fun_info(
                ScalarFunction("to_uint8", {LogicalType::INTEGER, LogicalType::VARCHAR}, LogicalType::UTINYINT,
                               ToUint8));
        abi_to_uint8_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        catalog.CreateFunction(*con.context, &abi_to_uint8_fun_info);

        CreateScalarFunctionInfo abi_to_uint16_fun_info(
                ScalarFunction("to_uint16", {LogicalType::INTEGER, LogicalType::VARCHAR}, LogicalType::USMALLINT,
                               ToUint16));
        abi_to_uint16_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        catalog.CreateFunction(*con.context, &abi_to_uint16_fun_info);

        CreateScalarFunctionInfo abi_to_uint32_fun_info(
                ScalarFunction("to_uint32", {LogicalType::INTEGER, LogicalType::VARCHAR}, LogicalType::UINTEGER,
                               ToUint32));
        abi_to_uint32_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        catalog.CreateFunction(*con.context, &abi_to_uint32_fun_info);

        CreateScalarFunctionInfo abi_to_uint64_fun_info(
                ScalarFunction("to_uint64", {LogicalType::INTEGER, LogicalType::VARCHAR}, LogicalType::UBIGINT,
                               ToUint64));
        abi_to_uint64_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        catalog.CreateFunction(*con.context, &abi_to_uint64_fun_info);

        CreateScalarFunctionInfo abi_to_uint128_fun_info(
                ScalarFunction("to_uint128", {LogicalType::INTEGER, LogicalType::VARCHAR},
                               LogicalType::LIST(LogicalType::UBIGINT),
                               ToUint128));
        abi_to_uint128_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        catalog.CreateFunction(*con.context, &abi_to_uint128_fun_info);

        CreateScalarFunctionInfo abi_to_uint256_fun_info(
                ScalarFunction("to_uint256", {LogicalType::INTEGER, LogicalType::VARCHAR},
                               LogicalType::LIST(LogicalType::UBIGINT),
                               ToUint256));
        abi_to_uint256_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        catalog.CreateFunction(*con.context, &abi_to_uint256_fun_info);

        CreateScalarFunctionInfo abi_to_int8_fun_info(
                ScalarFunction("to_int8", {LogicalType::INTEGER, LogicalType::VARCHAR}, LogicalType::TINYINT,
                               ToInt8));
        abi_to_int8_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        catalog.CreateFunction(*con.context, &abi_to_int8_fun_info);

        CreateScalarFunctionInfo abi_to_int16_fun_info(
                ScalarFunction("to_int16", {LogicalType::INTEGER, LogicalType::VARCHAR}, LogicalType::SMALLINT,
                               ToInt16));
        abi_to_int16_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        catalog.CreateFunction(*con.context, &abi_to_int16_fun_info);

        CreateScalarFunctionInfo abi_to_int32_fun_info(
                ScalarFunction("to_int32", {LogicalType::INTEGER, LogicalType::VARCHAR}, LogicalType::INTEGER,
                               ToInt32));
        abi_to_int32_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        catalog.CreateFunction(*con.context, &abi_to_int32_fun_info);

        CreateScalarFunctionInfo abi_to_int64_fun_info(
                ScalarFunction("to_int64", {LogicalType::INTEGER, LogicalType::VARCHAR}, LogicalType::BIGINT,
                               ToInt64));
        abi_to_int64_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        catalog.CreateFunction(*con.context, &abi_to_int64_fun_info);

        CreateScalarFunctionInfo abi_to_part_fun_info(
                ScalarFunction("to_part", {LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::INTEGER},
                               LogicalType::VARCHAR,
                               ToPart));
        abi_to_part_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        catalog.CreateFunction(*con.context, &abi_to_part_fun_info);

        CreateScalarFunctionInfo abi_to_address_fun_info(
                ScalarFunction("to_address", {LogicalType::INTEGER, LogicalType::VARCHAR}, LogicalType::VARCHAR,
                               ToAddress));
        abi_to_address_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        catalog.CreateFunction(*con.context, &abi_to_address_fun_info);

        CreateScalarFunctionInfo abi_blob_to_address_fun_info(
                ScalarFunction("blob_to_address", {LogicalType::INTEGER, LogicalType::BLOB}, LogicalType::VARCHAR,
                               BlobToAddress));
        abi_blob_to_address_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        catalog.CreateFunction(*con.context, &abi_blob_to_address_fun_info);

        CreateScalarFunctionInfo abi_to_string_fun_info(
                ScalarFunction("to_string", {LogicalType::INTEGER, LogicalType::VARCHAR}, LogicalType::VARCHAR,
                               ToString));
        abi_to_string_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        catalog.CreateFunction(*con.context, &abi_to_string_fun_info);

        CreateScalarFunctionInfo abi_blob_to_string_fun_info(
                ScalarFunction("blob_to_string", {LogicalType::INTEGER, LogicalType::BLOB}, LogicalType::VARCHAR,
                               BlobToString));
        abi_blob_to_string_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        catalog.CreateFunction(*con.context, &abi_blob_to_string_fun_info);

        CreateScalarFunctionInfo abi_blob_to_bytes_fun_info(
                ScalarFunction("blob_to_bytes", {LogicalType::INTEGER, LogicalType::BLOB}, LogicalType::VARCHAR,
                               BlobToBytes));
        abi_blob_to_bytes_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        catalog.CreateFunction(*con.context, &abi_blob_to_bytes_fun_info);

        con.Commit();
    }

    void AbiExtension::Load(DuckDB &db) {
        LoadInternal(*db.instance);
    }

    std::string AbiExtension::Name() {
        return "abi";
    }

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void abi_init(duckdb::DatabaseInstance &db) {
    LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *abi_version() {
    return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
