#define DUCKDB_EXTENSION_MAIN

#include "abi_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"


#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

    inline void Quack(DataChunk &args, ExpressionState &state, Vector &result) {
        auto &name_vector = args.data[0];
        UnaryExecutor::Execute<string_t, string_t>(
                name_vector, result, args.size(),
                [&](string_t name) {
                    return StringVector::AddString(result, "Quack " + name.GetString() + " 🐥");
                });
    }

    inline void QuackBinary(DataChunk &args, ExpressionState &state, Vector &result) {
        auto &name_vector = args.data[0];
        auto &lastname_vector = args.data[1];
        BinaryExecutor::Execute<string_t, string_t, string_t>(
                name_vector, lastname_vector, result, args.size(),
                [&](string_t name, string_t lastname) {
                    return StringVector::AddString(result,
                                                   "Quack " + name.GetString() + " " + lastname.GetString() + " 🐥");
                });
    }

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
                    return StringVector::AddString(result, Substr(pos, data, size));
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

        CreateScalarFunctionInfo abi_quack_info(
                ScalarFunction("quack", {LogicalType::VARCHAR}, LogicalType::VARCHAR, Quack));
        abi_quack_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        catalog.CreateFunction(*con.context, &abi_quack_info);

        CreateScalarFunctionInfo abi_quack_binary_info(
                ScalarFunction("quack_binary", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
                               QuackBinary));
        abi_quack_binary_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        catalog.CreateFunction(*con.context, &abi_quack_binary_info);

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
                ScalarFunction("to_part", {LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::INTEGER}, LogicalType::VARCHAR,
                               ToPart));
        abi_to_part_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        catalog.CreateFunction(*con.context, &abi_to_part_fun_info);

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
