#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionNumbers.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Common/typeid_cast.h>
#include <Common/FieldVisitorToString.h>
#include <Storages/System/StorageSystemNumbers.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypesNumber.h>
#include "registerTableFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


template <bool multithreaded>
ColumnsDescription TableFunctionNumbers<multithreaded>::getActualTableStructure(ContextPtr /*context*/) const
{
    /// NOTE: https://bugs.llvm.org/show_bug.cgi?id=47418
    return ColumnsDescription{{{"number", std::make_shared<DataTypeUInt64>()}}};
}

template <bool multithreaded>
StoragePtr TableFunctionNumbers<multithreaded>::executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    if (const auto * function = ast_function->as<ASTFunction>())
    {
        auto arguments = function->arguments->children;

        if (arguments.size() != 1 && arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' requires 'length' or 'offset, length'.", getName());

        UInt64 offset = arguments.size() == 2 ? evaluateArgument(context, arguments[0]) : 0;
        UInt64 length = arguments.size() == 2 ? evaluateArgument(context, arguments[1]) : evaluateArgument(context, arguments[0]);

        auto res = std::make_shared<StorageSystemNumbers>(StorageID(getDatabaseName(), table_name), multithreaded, length, offset, false);
        res->startup();
        return res;
    }
    throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' requires 'limit' or 'offset, limit'.", getName());
}

void registerTableFunctionNumbers(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionNumbers<true>>({.documentation = {}, .allow_readonly = true});
    factory.registerFunction<TableFunctionNumbers<false>>({.documentation = {}, .allow_readonly = true});
}

template <bool multithreaded>
UInt64 TableFunctionNumbers<multithreaded>::evaluateArgument(ContextPtr context, ASTPtr & argument) const
{
    const auto & [field, type] = evaluateConstantExpression(argument, context);

    if (!isNativeNumber(type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} expression, must be numeric type", type->getName());

    Field converted = convertFieldToType(field, DataTypeUInt64());
    if (converted.isNull())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The value {} is not representable as UInt64",
                        applyVisitor(FieldVisitorToString(), field));

    return converted.safeGet<UInt64>();
}

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
std::vector<int> generate_series(int start, int stop, int step = 1)
{
    if (step == 0 || start == NULL || stop == NULL) {
        throw std::invalid_argument("step cannot be zero, start and stop cannot be NULL");
    }
    if ((step > 0 && start > stop) || (step < 0 && start < stop)) {
        return std::vector<int>();
    }
    std::vector<int> result;
    int current = start;
    while ((step > 0 && current <= stop) || (step < 0 && current >= stop)) {
        result.push_back(current);
        current += step;
    }
    return result;
}

std::vector<long long> generate_series(long long start, long long stop, long long step = 1) {
    if (step == 0 || start == NULL || stop == NULL) {
        throw std::invalid_argument("step cannot be zero, start and stop cannot be NULL");
    }
    if ((step > 0 && start > stop) || (step < 0 && start < stop)) {
        return std::vector<long long>();
    }
    std::vector<long long> result;
    long long current = start;
    while ((step > 0 && current <= stop) || (step < 0 && current >= stop)) {
        result.push_back(current);
        current += step;
    }
    return result;
}

std::vector<double> generate_series(double start, double stop, double step = 1.0) {
    if (step == 0 || std::isnan(start) || std::isnan(stop)) {
        throw std::invalid_argument("step cannot be zero, start and stop cannot be NaN");
    }
    if ((step > 0 && start > stop) || (step < 0 && start < stop)) {
        return std::vector<double>();
    }
    std::vector<double> result;
    double current = start;
    while ((step > 0 && current <= stop) || (step < 0 && current >= stop)) {
        result.push_back(current);
        current += step;
    }
    return result;
}

std::vector<std::chrono::system_clock::time_point> generate_date_array(
        std::chrono::system_clock::time_point start, 
        std::chrono::system_clock::time_point stop, 
        std::chrono::milliseconds step = std::chrono::milliseconds(1))
    {
        if (step.count() == 0) {
        throw std::invalid_argument("step cannot be zero");
    }
    if (start > stop) {
        return std::vector<std::chrono::system_clock::time_point>();
    }
    std::vector<std::chrono::system_clock::time_point> result;
    std::chrono::system_clock::time_point current = start;
    while (current <= stop) {
        result.push_back(current);
        current += step;
    }
    return result;
    }

std::vector<std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>> generate_timestamp_array(
        std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> start, 
        std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> stop, 
        std::chrono::milliseconds step = std::chrono::milliseconds(1))
    {
        if (step.count() == 0) {
        throw std::invalid_argument("step cannot be zero");
    }
    if (start > stop) {
        return std::vector<std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>>();
    }
    std::vector<std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>> result;
    std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> current = start;
    while (current <= stop) {
        result.push_back(current);
        current += step;
    }
    return result;
    }
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
}
